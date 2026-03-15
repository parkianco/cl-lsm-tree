;;;; Copyright (c) 2024-2026 Parkian Company LLC. All rights reserved.
;;;; SPDX-License-Identifier: Apache-2.0

;;;; lsm.lisp - Main LSM-tree engine interface
;;;;
;;;; Complete LSM-tree storage engine with durability and recovery.

(in-package #:cl-lsm-tree)

;;; ============================================================================
;;; LSM Engine Structure
;;; ============================================================================

(defstruct (lsm-engine (:constructor %make-lsm-engine))
  "Complete LSM-tree storage engine.

   Provides durable key-value storage with:
   - High write throughput via log-structured design
   - Efficient reads via bloom filters and caching
   - Background compaction for space reclamation
   - Snapshot isolation for consistent reads"
  (directory nil :type (or null pathname))
  (memtable nil :type (or null memtable))
  (immutable-memtables nil :type list)
  (sstables nil :type list)
  (cache nil :type (or null block-cache))
  (wal nil)  ; Write-ahead log stream
  (wal-path nil :type (or null pathname))
  (compaction nil :type (or null compaction-state))
  (snapshots nil :type list)
  (next-sequence 0 :type fixnum)
  (next-snapshot-id 0 :type fixnum)
  (lock (sb-thread:make-mutex :name "lsm-engine") :type t)
  (open-p nil :type boolean)
  (stats-writes 0 :type fixnum)
  (stats-reads 0 :type fixnum)
  (stats-flushes 0 :type fixnum))

(defun make-lsm-engine (directory &key (cache-size (* 256 1024 1024)))
  "Create LSM engine for specified directory."
  (%make-lsm-engine
   :directory (pathname directory)
   :cache (make-block-cache cache-size)
   :compaction (make-compaction-state)))

;;; ============================================================================
;;; WAL (Write-Ahead Log)
;;; ============================================================================

(defun recover-from-wal (engine wal-path)
  "Recover memtable state from write-ahead log."
  (let ((mt (make-memtable)))
    (with-open-file (stream wal-path
                            :direction :input
                            :element-type '(unsigned-byte 8)
                            :if-does-not-exist nil)
      (when stream
        (handler-case
            (loop
              (let ((type (read-byte stream)))
                (case type
                  (1  ; PUT
                   (let ((klen (+ (read-byte stream) (ash (read-byte stream) 8)))
                         (vlen (+ (read-byte stream) (ash (read-byte stream) 8)
                                  (ash (read-byte stream) 16)
                                  (ash (read-byte stream) 24))))
                     (let ((key (make-array klen :element-type '(unsigned-byte 8)))
                           (value (make-array vlen :element-type '(unsigned-byte 8))))
                       (read-sequence key stream)
                       (read-sequence value stream)
                       (memtable-put mt key value))))
                  (2  ; DELETE
                   (let ((klen (+ (read-byte stream) (ash (read-byte stream) 8))))
                     (let ((key (make-array klen :element-type '(unsigned-byte 8))))
                       (read-sequence key stream)
                       (memtable-delete mt key)))))))
          (end-of-file ()))))
    (setf (lsm-engine-memtable engine) mt)))

(defun wal-write-put (wal key value)
  "Write PUT record to WAL."
  (write-byte 1 wal)  ; Type
  (let ((klen (length key))
        (vlen (length value)))
    (write-byte (ldb (byte 8 0) klen) wal)
    (write-byte (ldb (byte 8 8) klen) wal)
    (write-byte (ldb (byte 8 0) vlen) wal)
    (write-byte (ldb (byte 8 8) vlen) wal)
    (write-byte (ldb (byte 8 16) vlen) wal)
    (write-byte (ldb (byte 8 24) vlen) wal)
    (write-sequence key wal)
    (write-sequence value wal))
  (force-output wal))

(defun wal-write-delete (wal key)
  "Write DELETE record to WAL."
  (write-byte 2 wal)
  (let ((klen (length key)))
    (write-byte (ldb (byte 8 0) klen) wal)
    (write-byte (ldb (byte 8 8) klen) wal)
    (write-sequence key wal))
  (force-output wal))

;;; ============================================================================
;;; Engine Lifecycle
;;; ============================================================================

(defun engine-open (engine)
  "Open the LSM engine, recovering state from disk."
  (let ((dir (lsm-engine-directory engine)))
    ;; Create directory if needed
    (ensure-directories-exist (merge-pathnames "dummy" dir))
    ;; Open WAL
    (let ((wal-path (merge-pathnames "wal.log" dir)))
      (setf (lsm-engine-wal-path engine) wal-path)
      ;; Recover from WAL if exists
      (when (probe-file wal-path)
        (recover-from-wal engine wal-path))
      ;; Open new WAL
      (setf (lsm-engine-wal engine)
            (open wal-path
                  :direction :output
                  :if-exists :append
                  :if-does-not-exist :create
                  :element-type '(unsigned-byte 8))))
    ;; Load existing SSTables
    (let ((sst-files (directory (merge-pathnames "*.sst" dir))))
      (dolist (path sst-files)
        (push (open-sstable path) (lsm-engine-sstables engine))))
    ;; Initialize memtable if not recovered from WAL
    (unless (lsm-engine-memtable engine)
      (setf (lsm-engine-memtable engine) (make-memtable)))
    (setf (lsm-engine-open-p engine) t)
    engine))

(defun engine-close (engine)
  "Close the LSM engine, flushing all data to disk."
  (when (lsm-engine-open-p engine)
    (sb-thread:with-mutex ((lsm-engine-lock engine))
      ;; Stop compaction
      (when (lsm-engine-compaction engine)
        (setf (compaction-state-running-p (lsm-engine-compaction engine)) nil))
      ;; Flush memtable
      (when (> (memtable-count (lsm-engine-memtable engine)) 0)
        (flush-memtable engine))
      ;; Close WAL
      (when (lsm-engine-wal engine)
        (close (lsm-engine-wal engine))
        (setf (lsm-engine-wal engine) nil))
      ;; Close SSTables
      (dolist (sst (lsm-engine-sstables engine))
        (close-sstable sst))
      (setf (lsm-engine-open-p engine) nil)))
  t)

(defun engine-destroy (engine)
  "Destroy the engine and all its data."
  (engine-close engine)
  (let ((dir (lsm-engine-directory engine)))
    (dolist (file (directory (merge-pathnames "*.*" dir)))
      (delete-file file))))

(defmacro with-engine ((var directory &rest options) &body body)
  "Execute body with an open LSM engine."
  `(let ((,var (make-lsm-engine ,directory ,@options)))
     (unwind-protect
          (progn
            (engine-open ,var)
            ,@body)
       (engine-close ,var))))

;;; ============================================================================
;;; Core Operations
;;; ============================================================================

(defun engine-put (engine key value)
  "Put key-value pair into engine."
  (unless (lsm-engine-open-p engine)
    (error 'lsm-error :message "Engine not open"))
  (let ((key-bytes (if (stringp key)
                       (sb-ext:string-to-octets key :external-format :utf-8)
                       key))
        (value-bytes (if (stringp value)
                         (sb-ext:string-to-octets value :external-format :utf-8)
                         value)))
    (sb-thread:with-mutex ((lsm-engine-lock engine))
      ;; Write to WAL first
      (when (lsm-engine-wal engine)
        (wal-write-put (lsm-engine-wal engine) key-bytes value-bytes))
      ;; Write to memtable
      (memtable-put (lsm-engine-memtable engine) key-bytes value-bytes)
      (incf (lsm-engine-stats-writes engine))
      ;; Check if memtable needs flushing
      (when (>= (memtable-size (lsm-engine-memtable engine))
                +memtable-size-threshold+)
        (flush-memtable engine))))
  t)

(defun engine-get (engine key)
  "Get value for key. Returns (values value found-p)."
  (unless (lsm-engine-open-p engine)
    (error 'lsm-error :message "Engine not open"))
  (let ((key-bytes (if (stringp key)
                       (sb-ext:string-to-octets key :external-format :utf-8)
                       key)))
    (incf (lsm-engine-stats-reads engine))
    ;; Check memtable first
    (multiple-value-bind (value found deleted)
        (memtable-get (lsm-engine-memtable engine) key-bytes)
      (when found
        (return-from engine-get
          (if deleted
              (values nil nil)
              (values value t)))))
    ;; Check immutable memtables
    (dolist (imt (lsm-engine-immutable-memtables engine))
      (multiple-value-bind (value found deleted)
          (memtable-get imt key-bytes)
        (when found
          (return-from engine-get
            (if deleted
                (values nil nil)
                (values value t))))))
    ;; Check SSTables (newest first)
    (dolist (sst (sort (copy-list (lsm-engine-sstables engine))
                       #'> :key #'sstable-sequence))
      (multiple-value-bind (value found deleted)
          (sstable-get sst key-bytes (lsm-engine-cache engine))
        (when found
          (return-from engine-get
            (if deleted
                (values nil nil)
                (values value t))))))
    (values nil nil)))

(defun engine-delete (engine key)
  "Delete key from engine."
  (unless (lsm-engine-open-p engine)
    (error 'lsm-error :message "Engine not open"))
  (let ((key-bytes (if (stringp key)
                       (sb-ext:string-to-octets key :external-format :utf-8)
                       key)))
    (sb-thread:with-mutex ((lsm-engine-lock engine))
      ;; Write to WAL
      (when (lsm-engine-wal engine)
        (wal-write-delete (lsm-engine-wal engine) key-bytes))
      ;; Write tombstone to memtable
      (memtable-delete (lsm-engine-memtable engine) key-bytes)
      (incf (lsm-engine-stats-writes engine))))
  t)

(defun engine-contains-p (engine key)
  "Check if key exists in engine."
  (multiple-value-bind (value found)
      (engine-get engine key)
    (declare (ignore value))
    found))

;;; ============================================================================
;;; Range Scans
;;; ============================================================================

(defun engine-scan (engine &optional start-key end-key limit)
  "Scan entries in range. Returns list of (key . value) pairs."
  (unless (lsm-engine-open-p engine)
    (error 'lsm-error :message "Engine not open"))
  (let ((start-bytes (when start-key
                       (if (stringp start-key)
                           (sb-ext:string-to-octets start-key :external-format :utf-8)
                           start-key)))
        (end-bytes (when end-key
                     (if (stringp end-key)
                         (sb-ext:string-to-octets end-key :external-format :utf-8)
                         end-key)))
        (result nil)
        (seen (make-hash-table :test 'equalp))
        (count 0)
        (cmp #'key-compare))
    ;; Collect from memtable
    (let ((iter (memtable-iterator (lsm-engine-memtable engine) start-bytes)))
      (loop
        (multiple-value-bind (key value deleted seq valid)
            (funcall iter)
          (declare (ignore seq))
          (unless valid (return))
          (when (and end-bytes (>= (funcall cmp key end-bytes) 0))
            (return))
          (when (and limit (>= count limit)) (return))
          (unless (gethash key seen)
            (setf (gethash key seen) t)
            (unless deleted
              (push (cons key value) result)
              (incf count))))))
    ;; Collect from immutable memtables
    (dolist (imt (lsm-engine-immutable-memtables engine))
      (when (and limit (>= count limit)) (return))
      (let ((iter (memtable-iterator imt start-bytes)))
        (loop
          (multiple-value-bind (key value deleted seq valid)
              (funcall iter)
            (declare (ignore seq))
            (unless valid (return))
            (when (and end-bytes (>= (funcall cmp key end-bytes) 0))
              (return))
            (when (and limit (>= count limit)) (return))
            (unless (gethash key seen)
              (setf (gethash key seen) t)
              (unless deleted
                (push (cons key value) result)
                (incf count)))))))
    ;; Collect from SSTables
    (dolist (sst (sort (copy-list (lsm-engine-sstables engine))
                       #'> :key #'sstable-sequence))
      (when (and limit (>= count limit)) (return))
      ;; Check key range overlap
      (when (or (and start-bytes (sstable-max-key sst)
                     (< (funcall cmp (sstable-max-key sst) start-bytes) 0))
                (and end-bytes (sstable-min-key sst)
                     (>= (funcall cmp (sstable-min-key sst) end-bytes) 0)))
        (return))  ; No overlap, skip
      ;; Read SSTable entries
      (with-open-file (stream (sstable-path sst)
                              :direction :input
                              :element-type '(unsigned-byte 8))
        (file-position stream 4)  ; Skip magic
        (handler-case
            (loop
              (let ((block-data (make-array +block-size+
                                            :element-type '(unsigned-byte 8))))
                (when (zerop (read-sequence block-data stream))
                  (return))
                (let ((block (block-deserialize block-data)))
                  (dolist (entry (data-block-entries block))
                    (when (and limit (>= count limit)) (return))
                    (destructuring-bind (key value deleted) entry
                      ;; Skip keys before start-bytes
                      (unless (and start-bytes (< (funcall cmp key start-bytes) 0))
                        ;; Return when we reach end-bytes
                        (when (and end-bytes (>= (funcall cmp key end-bytes) 0))
                          (return))
                        ;; Process if not already seen
                        (unless (gethash key seen)
                          (setf (gethash key seen) t)
                          (unless deleted
                            (push (cons key value) result)
                            (incf count)))))))))
          (end-of-file ()))))
    ;; Sort results
    (sort result (lambda (a b) (< (funcall cmp (car a) (car b)) 0)))))

(defun engine-scan-range (engine start-key end-key)
  "Scan range [start-key, end-key)."
  (engine-scan engine start-key end-key nil))

;;; ============================================================================
;;; Batch Operations
;;; ============================================================================

(defun batch-apply (engine batch)
  "Apply batch of operations atomically."
  (sb-thread:with-mutex ((lsm-engine-lock engine))
    (dolist (op (reverse (write-batch-operations batch)))
      (ecase (first op)
        (:put
         (engine-put engine (second op) (third op)))
        (:delete
         (engine-delete engine (second op)))))))

;;; ============================================================================
;;; Snapshots
;;; ============================================================================

(defun create-snapshot (engine)
  "Create point-in-time snapshot for consistent reads."
  (sb-thread:with-mutex ((lsm-engine-lock engine))
    (let ((snap (%make-snapshot
                 :id (incf (lsm-engine-next-snapshot-id engine))
                 :sequence-number (memtable-sequence-number
                                   (lsm-engine-memtable engine))
                 :memtable (lsm-engine-memtable engine)
                 :immutable-memtables (copy-list
                                       (lsm-engine-immutable-memtables engine))
                 :sstables (copy-list (lsm-engine-sstables engine)))))
      (push snap (lsm-engine-snapshots engine))
      snap)))

(defun release-snapshot (engine snapshot)
  "Release a snapshot, allowing garbage collection."
  (sb-thread:with-mutex ((lsm-engine-lock engine))
    (setf (snapshot-valid-p snapshot) nil)
    (setf (lsm-engine-snapshots engine)
          (remove snapshot (lsm-engine-snapshots engine))))
  t)

(defun snapshot-get (snapshot key)
  "Get value for key at snapshot point."
  (unless (snapshot-valid-p snapshot)
    (error 'lsm-error :message "Snapshot has been released"))
  (let ((key-bytes (if (stringp key)
                       (sb-ext:string-to-octets key :external-format :utf-8)
                       key))
        (seq (snapshot-sequence-number snapshot)))
    ;; Check memtable entries up to snapshot sequence
    (let ((iter (memtable-iterator (snapshot-memtable snapshot) key-bytes)))
      (loop
        (multiple-value-bind (k value deleted entry-seq valid)
            (funcall iter)
          (unless valid (return))
          (when (equalp k key-bytes)
            (when (<= entry-seq seq)
              (return-from snapshot-get
                (if deleted
                    (values nil nil)
                    (values value t))))
            (return)))))
    ;; Check immutable memtables
    (dolist (imt (snapshot-immutable-memtables snapshot))
      (multiple-value-bind (value found deleted)
          (memtable-get imt key-bytes)
        (when found
          (return-from snapshot-get
            (if deleted
                (values nil nil)
                (values value t))))))
    ;; Check SSTables
    (dolist (sst (snapshot-sstables snapshot))
      (multiple-value-bind (value found deleted)
          (sstable-get sst key-bytes nil)
        (when found
          (return-from snapshot-get
            (if deleted
                (values nil nil)
                (values value t))))))
    (values nil nil)))

(defun snapshot-scan (snapshot &optional start-key end-key)
  "Scan entries in range at snapshot point."
  (unless (snapshot-valid-p snapshot)
    (error 'lsm-error :message "Snapshot has been released"))
  ;; Similar to engine-scan but respects snapshot sequence number
  (let ((result nil))
    ;; Simplified implementation - full version would filter by sequence
    (let ((iter (memtable-iterator (snapshot-memtable snapshot) start-key))
          (cmp #'key-compare))
      (loop
        (multiple-value-bind (key value deleted seq valid)
            (funcall iter)
          (declare (ignore seq))
          (unless valid (return))
          (when (and end-key (>= (funcall cmp key end-key) 0))
            (return))
          (unless deleted
            (push (cons key value) result)))))
    (nreverse result)))

;;; ============================================================================
;;; Flush & Compaction
;;; ============================================================================

(defun flush-memtable (engine)
  "Flush current memtable to SSTable."
  (let ((mt (lsm-engine-memtable engine)))
    (memtable-freeze mt)
    ;; Collect all entries
    (let ((entries nil)
          (iter (memtable-iterator mt)))
      (loop
        (multiple-value-bind (key value deleted seq valid)
            (funcall iter)
          (declare (ignore seq))
          (unless valid (return))
          (push (list key value deleted) entries)))
      (setf entries (nreverse entries))
      ;; Write SSTable at level 0
      (when entries
        (let ((sst (write-sstable (lsm-engine-directory engine)
                                  0  ; Level 0
                                  (incf (lsm-engine-next-sequence engine))
                                  entries)))
          (push sst (lsm-engine-sstables engine)))))
    ;; Replace with new memtable
    (push mt (lsm-engine-immutable-memtables engine))
    (setf (lsm-engine-memtable engine) (make-memtable))
    ;; Truncate WAL
    (when (lsm-engine-wal engine)
      (close (lsm-engine-wal engine))
      (delete-file (lsm-engine-wal-path engine))
      (setf (lsm-engine-wal engine)
            (open (lsm-engine-wal-path engine)
                  :direction :output
                  :if-exists :supersede
                  :element-type '(unsigned-byte 8))))
    ;; Clean up old immutable memtables
    (setf (lsm-engine-immutable-memtables engine)
          (butlast (lsm-engine-immutable-memtables engine)
                   (max 0 (- (length (lsm-engine-immutable-memtables engine)) 2))))
    (incf (lsm-engine-stats-flushes engine))))

(defun run-compaction (engine level)
  "Run compaction for specified level."
  (let* ((inputs (pick-compaction-inputs (lsm-engine-sstables engine)
                                         level
                                         (compaction-state-strategy
                                          (lsm-engine-compaction engine))))
         (merged (when inputs (merge-sstables inputs))))
    (when merged
      ;; Deduplicate (keep latest version of each key)
      (let ((deduped nil)
            (prev-key nil)
            (cmp #'key-compare))
        (dolist (entry merged)
          (let ((key (first entry)))
            (unless (and prev-key (zerop (funcall cmp prev-key key)))
              (push entry deduped)
              (setf prev-key key))))
        (setf merged (nreverse deduped)))
      ;; Write new SSTable at next level
      (let ((new-sst (write-sstable (lsm-engine-directory engine)
                                    (1+ level)
                                    (incf (lsm-engine-next-sequence engine))
                                    merged)))
        ;; Update engine state
        (sb-thread:with-mutex ((lsm-engine-lock engine))
          ;; Remove old SSTables
          (dolist (old inputs)
            (setf (lsm-engine-sstables engine)
                  (remove old (lsm-engine-sstables engine)))
            (delete-file (sstable-path old)))
          ;; Add new SSTable
          (push new-sst (lsm-engine-sstables engine)))))))

(defun trigger-compaction (engine &optional level)
  "Trigger background compaction."
  (let ((state (lsm-engine-compaction engine)))
    (when state
      (setf (compaction-state-pending-p state) t)
      (unless (compaction-state-running-p state)
        (setf (compaction-state-running-p state) t)
        (sb-thread:make-thread
         (lambda ()
           (unwind-protect
                (run-compaction engine (or level 0))
             (setf (compaction-state-running-p state) nil
                   (compaction-state-pending-p state) nil)))
         :name "lsm-compaction")))))

(defun await-compaction (engine)
  "Wait for pending compaction to complete."
  (let ((state (lsm-engine-compaction engine)))
    (when state
      (loop while (or (compaction-state-running-p state)
                      (compaction-state-pending-p state))
            do (sleep 0.1))))
  t)

(defun set-compaction-strategy (engine strategy)
  "Set compaction strategy (:leveled, :tiered, or :fifo)."
  (let ((state (lsm-engine-compaction engine)))
    (when state
      (setf (compaction-state-strategy state) strategy))))

;;; ============================================================================
;;; Statistics
;;; ============================================================================

(defun engine-statistics (engine)
  "Return engine statistics as plist."
  (list :writes (lsm-engine-stats-writes engine)
        :reads (lsm-engine-stats-reads engine)
        :flushes (lsm-engine-stats-flushes engine)
        :memtable-size (memtable-size (lsm-engine-memtable engine))
        :memtable-count (memtable-count (lsm-engine-memtable engine))
        :sstable-count (length (lsm-engine-sstables engine))
        :cache-hit-rate (when (lsm-engine-cache engine)
                          (cache-hit-rate (lsm-engine-cache engine)))
        :snapshot-count (length (lsm-engine-snapshots engine))))

(defun sstable-count (engine)
  "Return number of SSTables."
  (length (lsm-engine-sstables engine)))

;;; end of lsm.lisp
