;;;; sstable.lisp - On-disk sorted string table
;;;;
;;;; SSTable implementation with block-based layout for persistent storage.

(in-package #:cl-lsm-tree)

;;; ============================================================================
;;; Data Block
;;; ============================================================================

(defstruct (data-block (:constructor %make-data-block))
  "Block of key-value entries within SSTable.

   Uses prefix compression with periodic restart points
   for efficient range scans."
  (entries nil :type list)
  (restarts nil :type (or null simple-vector))
  (size 0 :type fixnum))

(defun make-data-block ()
  "Create empty data block."
  (%make-data-block))

(defun block-add-entry (block key value deleted-p)
  "Add entry to data block."
  (push (list key value deleted-p) (data-block-entries block))
  (incf (data-block-size block)
        (+ (length key) (if value (length value) 0) 8)))

(defun block-finish (block)
  "Finalize block, reversing entries and computing restarts."
  (setf (data-block-entries block)
        (nreverse (data-block-entries block)))
  ;; Compute restart points
  (let ((count (length (data-block-entries block))))
    (setf (data-block-restarts block)
          (coerce (loop for i below count by +restart-interval+
                        collect i)
                  'simple-vector))))

(defun block-serialize (block)
  "Serialize block to byte vector."
  (let ((buf (make-array 4096
                         :element-type '(unsigned-byte 8)
                         :fill-pointer 0
                         :adjustable t)))
    ;; Entry count (4 bytes)
    (let ((count (length (data-block-entries block))))
      (loop for i below 4
            do (vector-push-extend (ldb (byte 8 (* i 8)) count) buf)))
    ;; Entries
    (dolist (entry (data-block-entries block))
      (destructuring-bind (key value deleted-p) entry
        ;; Key length (2 bytes) + key
        (let ((klen (length key)))
          (vector-push-extend (ldb (byte 8 0) klen) buf)
          (vector-push-extend (ldb (byte 8 8) klen) buf))
        (loop for byte across key do (vector-push-extend byte buf))
        ;; Deleted flag (1 byte)
        (vector-push-extend (if deleted-p 1 0) buf)
        ;; Value length (4 bytes) + value
        (if (and value (not deleted-p))
            (let ((vlen (length value)))
              (loop for i below 4
                    do (vector-push-extend (ldb (byte 8 (* i 8)) vlen) buf))
              (loop for byte across value do (vector-push-extend byte buf)))
            (loop for i below 4 do (vector-push-extend 0 buf)))))
    ;; Restart count (4 bytes) + restart offsets
    (let ((rcount (length (data-block-restarts block))))
      (loop for i below 4
            do (vector-push-extend (ldb (byte 8 (* i 8)) rcount) buf))
      (loop for r across (data-block-restarts block)
            do (loop for i below 4
                     do (vector-push-extend (ldb (byte 8 (* i 8)) r) buf))))
    (coerce buf '(simple-array (unsigned-byte 8) (*)))))

(defun block-deserialize (bytes)
  "Deserialize block from byte vector."
  (let ((pos 0)
        (block (make-data-block)))
    ;; Entry count
    (let ((count (+ (aref bytes 0)
                    (ash (aref bytes 1) 8)
                    (ash (aref bytes 2) 16)
                    (ash (aref bytes 3) 24))))
      (incf pos 4)
      ;; Read entries
      (dotimes (i count)
        ;; Key length + key
        (let ((klen (+ (aref bytes pos) (ash (aref bytes (1+ pos)) 8))))
          (incf pos 2)
          (let ((key (make-array klen :element-type '(unsigned-byte 8))))
            (replace key bytes :start2 pos)
            (incf pos klen)
            ;; Deleted flag
            (let ((deleted-p (= 1 (aref bytes pos))))
              (incf pos)
              ;; Value length + value
              (let ((vlen (+ (aref bytes pos)
                             (ash (aref bytes (+ pos 1)) 8)
                             (ash (aref bytes (+ pos 2)) 16)
                             (ash (aref bytes (+ pos 3)) 24))))
                (incf pos 4)
                (let ((value (when (> vlen 0)
                               (let ((v (make-array vlen :element-type '(unsigned-byte 8))))
                                 (replace v bytes :start2 pos)
                                 v))))
                  (incf pos vlen)
                  (push (list key value deleted-p)
                        (data-block-entries block)))))))))
    (setf (data-block-entries block)
          (nreverse (data-block-entries block)))
    block))

;;; ============================================================================
;;; SSTable
;;; ============================================================================

(defstruct (sstable (:constructor %make-sstable))
  "Sorted String Table - immutable on-disk sorted key-value store.

   Format:
   [Data Blocks][Index Block][Bloom Filter][Footer]

   Footer contains offsets to index and bloom filter."
  (path nil :type (or null pathname))
  (level 0 :type fixnum)
  (sequence 0 :type fixnum)
  (min-key nil :type t)
  (max-key nil :type t)
  (entry-count 0 :type fixnum)
  (file-size 0 :type fixnum)
  (index nil :type list)
  (bloom nil :type (or null bloom-filter))
  (stream nil))

(defun sstable-filename (directory level sequence)
  "Generate SSTable filename."
  (merge-pathnames (format nil "L~D-~8,'0D.sst" level sequence)
                   directory))

(defun write-sstable (directory level sequence entries)
  "Write entries to new SSTable file. Returns SSTable structure."
  (let* ((path (sstable-filename directory level sequence))
         (bloom (make-bloom-filter (length entries)))
         (index nil)
         (block (make-data-block))
         (block-offset 0)
         (min-key nil)
         (max-key nil))
    (with-open-file (stream path
                            :direction :output
                            :if-exists :supersede
                            :element-type '(unsigned-byte 8))
      ;; Write magic number
      (loop for i below 4
            do (write-byte (ldb (byte 8 (* i 8)) +sstable-magic+) stream))
      (setf block-offset 4)
      ;; Build blocks and index
      (dolist (entry entries)
        (destructuring-bind (key value deleted-p) entry
          (unless min-key (setf min-key key))
          (setf max-key key)
          (bloom-add bloom key)
          (block-add-entry block key value deleted-p)
          ;; Flush block if large enough
          (when (>= (data-block-size block) +block-size+)
            (block-finish block)
            (let ((bytes (block-serialize block)))
              (write-sequence bytes stream)
              (push (cons (first (first (data-block-entries block)))
                          block-offset)
                    index)
              (incf block-offset (length bytes)))
            (setf block (make-data-block)))))
      ;; Write final block
      (when (data-block-entries block)
        (block-finish block)
        (let ((bytes (block-serialize block)))
          (write-sequence bytes stream)
          (push (cons (first (first (data-block-entries block)))
                      block-offset)
                index)
          (incf block-offset (length bytes))))
      ;; Write index
      (let ((index-offset block-offset))
        (setf index (nreverse index))
        (loop for i below 4
              do (write-byte (ldb (byte 8 (* i 8)) (length index)) stream))
        (incf block-offset 4)
        (dolist (entry index)
          (let ((key (car entry))
                (offset (cdr entry)))
            (let ((klen (length key)))
              (loop for i below 2
                    do (write-byte (ldb (byte 8 (* i 8)) klen) stream))
              (write-sequence key stream)
              (loop for i below 4
                    do (write-byte (ldb (byte 8 (* i 8)) offset) stream))
              (incf block-offset (+ 6 klen)))))
        ;; Write bloom filter
        (let ((bloom-offset block-offset)
              (bloom-bytes (bloom-serialize bloom)))
          (write-sequence bloom-bytes stream)
          (incf block-offset (length bloom-bytes))
          ;; Write footer
          (loop for i below 8
                do (write-byte (ldb (byte 8 (* i 8)) index-offset) stream))
          (loop for i below 8
                do (write-byte (ldb (byte 8 (* i 8)) bloom-offset) stream)))))
    ;; Return SSTable structure
    (%make-sstable
     :path path
     :level level
     :sequence sequence
     :min-key min-key
     :max-key max-key
     :entry-count (length entries)
     :file-size (with-open-file (s path :direction :input
                                         :element-type '(unsigned-byte 8))
                  (file-length s))
     :index index
     :bloom bloom)))

(defun open-sstable (path)
  "Open existing SSTable file and load metadata."
  (let ((sst (%make-sstable :path (pathname path))))
    (with-open-file (stream path
                            :direction :input
                            :element-type '(unsigned-byte 8))
      (setf (sstable-file-size sst) (file-length stream))
      ;; Read footer (last 16 bytes)
      (file-position stream (- (file-length stream) 16))
      (let ((footer (make-array 16 :element-type '(unsigned-byte 8))))
        (read-sequence footer stream)
        (let ((index-offset (+ (aref footer 0)
                               (ash (aref footer 1) 8)
                               (ash (aref footer 2) 16)
                               (ash (aref footer 3) 24)
                               (ash (aref footer 4) 32)
                               (ash (aref footer 5) 40)
                               (ash (aref footer 6) 48)
                               (ash (aref footer 7) 56)))
              (bloom-offset (+ (aref footer 8)
                               (ash (aref footer 9) 8)
                               (ash (aref footer 10) 16)
                               (ash (aref footer 11) 24)
                               (ash (aref footer 12) 32)
                               (ash (aref footer 13) 40)
                               (ash (aref footer 14) 48)
                               (ash (aref footer 15) 56))))
          ;; Read bloom filter
          (file-position stream bloom-offset)
          (let ((bloom-size (- (- (file-length stream) 16) bloom-offset)))
            (when (> bloom-size 0)
              (let ((bloom-bytes (make-array bloom-size
                                             :element-type '(unsigned-byte 8))))
                (read-sequence bloom-bytes stream)
                (setf (sstable-bloom sst) (bloom-deserialize bloom-bytes)))))
          ;; Read index
          (file-position stream index-offset)
          (let ((count-bytes (make-array 4 :element-type '(unsigned-byte 8))))
            (read-sequence count-bytes stream)
            (let ((count (+ (aref count-bytes 0)
                            (ash (aref count-bytes 1) 8)
                            (ash (aref count-bytes 2) 16)
                            (ash (aref count-bytes 3) 24))))
              (dotimes (i count)
                (let ((klen-bytes (make-array 2 :element-type '(unsigned-byte 8))))
                  (read-sequence klen-bytes stream)
                  (let* ((klen (+ (aref klen-bytes 0) (ash (aref klen-bytes 1) 8)))
                         (key (make-array klen :element-type '(unsigned-byte 8)))
                         (off-bytes (make-array 4 :element-type '(unsigned-byte 8))))
                    (read-sequence key stream)
                    (read-sequence off-bytes stream)
                    (let ((offset (+ (aref off-bytes 0)
                                     (ash (aref off-bytes 1) 8)
                                     (ash (aref off-bytes 2) 16)
                                     (ash (aref off-bytes 3) 24))))
                      (push (cons key offset) (sstable-index sst))))))
              (setf (sstable-index sst) (nreverse (sstable-index sst)))
              (when (sstable-index sst)
                (setf (sstable-min-key sst) (caar (sstable-index sst))
                      (sstable-max-key sst) (caar (last (sstable-index sst))))))))))
    sst))

(defun sstable-get (sst key cache)
  "Look up key in SSTable. Returns (values value found-p deleted-p)."
  ;; Check bloom filter first
  (when (and (sstable-bloom sst)
             (not (bloom-may-contain (sstable-bloom sst) key)))
    (return-from sstable-get (values nil nil nil)))
  ;; Binary search index for block
  (let* ((index (sstable-index sst))
         (cmp #'key-compare)
         (block-offset nil))
    (loop for i from (1- (length index)) downto 0
          for entry = (nth i index)
          when (<= (funcall cmp (car entry) key) 0)
            do (setf block-offset (cdr entry))
               (return))
    (unless block-offset
      (return-from sstable-get (values nil nil nil)))
    ;; Check cache
    (let ((cache-key (cons (sstable-path sst) block-offset)))
      (let ((cached (when cache (cache-get cache cache-key))))
        (if cached
            ;; Search cached block
            (dolist (entry (data-block-entries cached)
                           (values nil nil nil))
              (destructuring-bind (k v deleted) entry
                (when (zerop (funcall cmp k key))
                  (return (values v t deleted)))))
            ;; Read from disk
            (with-open-file (stream (sstable-path sst)
                                    :direction :input
                                    :element-type '(unsigned-byte 8))
              (file-position stream block-offset)
              ;; Read block size (first 4 bytes of entry count tell us entries)
              (let ((header (make-array 4 :element-type '(unsigned-byte 8))))
                (read-sequence header stream)
                (file-position stream block-offset)
                ;; Read full block (estimate size)
                (let* ((block-data (make-array +block-size+
                                               :element-type '(unsigned-byte 8)))
                       (bytes-read (read-sequence block-data stream)))
                  (declare (ignore bytes-read))
                  (let ((block (block-deserialize block-data)))
                    ;; Cache the block
                    (when cache
                      (cache-put cache cache-key block +block-size+))
                    ;; Search block
                    (dolist (entry (data-block-entries block)
                                   (values nil nil nil))
                      (destructuring-bind (k v deleted) entry
                        (when (zerop (funcall cmp k key))
                          (return (values v t deleted))))))))))))))

(defun close-sstable (sst)
  "Close SSTable and release resources."
  (when (sstable-stream sst)
    (close (sstable-stream sst))
    (setf (sstable-stream sst) nil)))

;;; end of sstable.lisp
