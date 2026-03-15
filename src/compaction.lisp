;;;; Copyright (c) 2024-2026 Parkian Company LLC. All rights reserved.
;;;; SPDX-License-Identifier: Apache-2.0

;;;; compaction.lisp - SSTable merging and compaction
;;;;
;;;; Background compaction strategies for LSM-tree maintenance.

(in-package #:cl-lsm-tree)

;;; ============================================================================
;;; Compaction Strategy
;;; ============================================================================

(deftype compaction-strategy ()
  '(member :leveled :tiered :fifo))

(defstruct (compaction-state (:constructor make-compaction-state))
  "State for background compaction."
  (strategy :leveled :type compaction-strategy)
  (running-p nil :type boolean)
  (pending-p nil :type boolean)
  (thread nil)
  (lock (sb-thread:make-mutex :name "compaction") :type t)
  (condvar (sb-thread:make-waitqueue :name "compaction-cv")))

;;; ============================================================================
;;; Compaction Selection
;;; ============================================================================

(defun pick-compaction-inputs (sstables level strategy)
  "Select SSTables for compaction based on strategy."
  (declare (ignore strategy))
  (let ((level-files (remove-if-not (lambda (s) (= (sstable-level s) level))
                                    sstables)))
    (when (> (length level-files) 4)  ; Trigger at 4 files per level
      ;; Pick oldest files
      (subseq (sort (copy-list level-files) #'<
                    :key #'sstable-sequence)
              0 (min 4 (length level-files))))))

;;; ============================================================================
;;; Merge Algorithm
;;; ============================================================================

(defun merge-sstables (inputs)
  "Merge multiple SSTables into single sorted stream of entries."
  (let ((iterators nil)
        (cmp #'key-compare))
    ;; Create iterators for each SSTable
    (dolist (sst inputs)
      (with-open-file (stream (sstable-path sst)
                              :direction :input
                              :element-type '(unsigned-byte 8))
        ;; Skip magic
        (file-position stream 4)
        (handler-case
            (loop
              (let ((block-data (make-array +block-size+
                                            :element-type '(unsigned-byte 8))))
                (when (zerop (read-sequence block-data stream))
                  (return))
                (let ((block (block-deserialize block-data)))
                  (dolist (entry (data-block-entries block))
                    (push entry iterators)))))
          (end-of-file ()))))
    ;; Sort all entries
    (stable-sort iterators (lambda (a b)
                             (< (funcall cmp (first a) (first b)) 0)))))

;;; ============================================================================
;;; Write Batch
;;; ============================================================================

(defstruct (write-batch (:constructor make-write-batch))
  "Atomic batch of write operations."
  (operations nil :type list)
  (size 0 :type fixnum))

(defun batch-put (batch key value)
  "Add put operation to batch."
  (push (list :put key value) (write-batch-operations batch))
  (incf (write-batch-size batch)
        (+ (length key) (if value (length value) 0))))

(defun batch-delete (batch key)
  "Add delete operation to batch."
  (push (list :delete key) (write-batch-operations batch))
  (incf (write-batch-size batch) (length key)))

(defun batch-clear (batch)
  "Clear all operations from batch."
  (setf (write-batch-operations batch) nil
        (write-batch-size batch) 0))

;;; ============================================================================
;;; Snapshot
;;; ============================================================================

(defstruct (snapshot (:constructor %make-snapshot))
  "Point-in-time read view of the database."
  (id 0 :type fixnum)
  (sequence-number 0 :type fixnum)
  (memtable nil)
  (immutable-memtables nil :type list)
  (sstables nil :type list)
  (valid-p t :type boolean)
  (create-time (get-internal-real-time) :type fixnum))

;;; end of compaction.lisp
