;;;; Copyright (c) 2024-2026 Parkian Company LLC. All rights reserved.
;;;; SPDX-License-Identifier: Apache-2.0

;;;; memtable.lisp - In-memory sorted table with skip list
;;;;
;;;; MemTable implementation using a probabilistic skip list data structure.

(in-package #:cl-lsm-tree)

;;; ============================================================================
;;; Skip List Node
;;; ============================================================================

(defstruct (skip-node (:constructor %make-skip-node))
  "Node in skip list data structure."
  (key nil :type t)
  (value nil :type t)
  (forward nil :type simple-vector)
  (sequence-number 0 :type fixnum)
  (deleted-p nil :type boolean))

(defun make-skip-node (key value height &optional (seq 0))
  "Create a skip list node with specified height."
  (%make-skip-node
   :key key
   :value value
   :forward (make-array height :initial-element nil)
   :sequence-number seq))

;;; ============================================================================
;;; Skip List
;;; ============================================================================

(defstruct (skip-list (:constructor %make-skip-list))
  "Probabilistic balanced search structure for MemTable.

   Properties:
   - O(log n) insert, delete, search
   - Supports range queries efficiently
   - Lock-free reads possible with careful implementation"
  (head nil :type (or null skip-node))
  (height 1 :type fixnum)
  (size 0 :type fixnum)
  (bytes 0 :type fixnum)
  (comparator #'key-compare :type function)
  (random-state (make-random-state t) :type random-state)
  (lock (sb-thread:make-mutex :name "skip-list") :type t))

(defun make-skip-list (&key (comparator #'key-compare))
  "Create an empty skip list."
  (let ((head (make-skip-node nil nil +max-skip-list-height+)))
    (%make-skip-list :head head :comparator comparator)))

(defun random-height (random-state)
  "Generate random height for new node."
  (do ((height 1 (1+ height)))
      ((or (>= height +max-skip-list-height+)
           (>= (random 1.0 random-state) +skip-list-probability+))
       height)))

(defun skip-list-find-path (list key)
  "Find path to key position, returning update vector."
  (let ((update (make-array +max-skip-list-height+ :initial-element nil))
        (node (skip-list-head list))
        (cmp (skip-list-comparator list)))
    (loop for level from (1- (skip-list-height list)) downto 0
          do (loop while (let ((next (aref (skip-node-forward node) level)))
                           (and next (< (funcall cmp (skip-node-key next) key) 0)))
                   do (setf node (aref (skip-node-forward node) level)))
             (setf (aref update level) node))
    (values (aref (skip-node-forward node) 0) update)))

(defun skip-list-insert (list key value sequence-number)
  "Insert or update key-value pair. Returns previous value if any."
  (sb-thread:with-mutex ((skip-list-lock list))
    (multiple-value-bind (node update)
        (skip-list-find-path list key)
      (let ((cmp (skip-list-comparator list)))
        (if (and node (zerop (funcall cmp (skip-node-key node) key)))
            ;; Update existing
            (let ((old-value (skip-node-value node)))
              (setf (skip-node-value node) value
                    (skip-node-sequence-number node) sequence-number
                    (skip-node-deleted-p node) nil)
              old-value)
            ;; Insert new
            (let* ((height (random-height (skip-list-random-state list)))
                   (new-node (make-skip-node key value height sequence-number)))
              ;; Grow list height if needed
              (when (> height (skip-list-height list))
                (loop for i from (skip-list-height list) below height
                      do (setf (aref update i) (skip-list-head list)))
                (setf (skip-list-height list) height))
              ;; Link node at all levels
              (loop for i below height
                    for prev = (aref update i)
                    do (setf (aref (skip-node-forward new-node) i)
                             (aref (skip-node-forward prev) i))
                       (setf (aref (skip-node-forward prev) i) new-node))
              (incf (skip-list-size list))
              (incf (skip-list-bytes list)
                    (+ (if (vectorp key) (length key) 8)
                       (if (vectorp value) (length value) 8)
                       64))  ; Node overhead
              nil))))))

(defun skip-list-get (list key)
  "Get value for key. Returns (values value found-p)."
  (sb-thread:with-mutex ((skip-list-lock list))
    (let ((node (skip-list-find-path list key))
          (cmp (skip-list-comparator list)))
      (if (and node
               (zerop (funcall cmp (skip-node-key node) key))
               (not (skip-node-deleted-p node)))
          (values (skip-node-value node) t)
          (values nil nil)))))

(defun skip-list-delete (list key sequence-number)
  "Mark key as deleted (tombstone). Returns T if key existed."
  (sb-thread:with-mutex ((skip-list-lock list))
    (let ((node (skip-list-find-path list key))
          (cmp (skip-list-comparator list)))
      (when (and node (zerop (funcall cmp (skip-node-key node) key)))
        (setf (skip-node-deleted-p node) t
              (skip-node-sequence-number node) sequence-number)
        t))))

(defun skip-list-iterator (list &optional start-key)
  "Create iterator over skip list entries."
  (let ((current (if start-key
                     (skip-list-find-path list start-key)
                     (aref (skip-node-forward (skip-list-head list)) 0))))
    (lambda ()
      (when current
        (let ((key (skip-node-key current))
              (value (skip-node-value current))
              (deleted (skip-node-deleted-p current))
              (seq (skip-node-sequence-number current)))
          (setf current (aref (skip-node-forward current) 0))
          (values key value deleted seq t))))))

(defun skip-list-range (list start-key end-key)
  "Get entries in range [start-key, end-key)."
  (let ((result nil)
        (cmp (skip-list-comparator list)))
    (sb-thread:with-mutex ((skip-list-lock list))
      (let ((iter (skip-list-iterator list start-key)))
        (loop
          (multiple-value-bind (key value deleted seq valid)
              (funcall iter)
            (declare (ignore seq))
            (unless valid (return))
            (when (and end-key (>= (funcall cmp key end-key) 0))
              (return))
            (unless deleted
              (push (list key value) result))))))
    (nreverse result)))

;;; ============================================================================
;;; MemTable
;;; ============================================================================

(defstruct (memtable (:constructor %make-memtable))
  "In-memory write buffer backed by skip list.

   Writes are accumulated here before being flushed to SSTable.
   Supports both mutable and immutable (frozen) states."
  (skiplist nil :type (or null skip-list))
  (sequence-number 0 :type fixnum)
  (frozen-p nil :type boolean)
  (id 0 :type fixnum)
  (create-time (get-internal-real-time) :type fixnum))

(defun make-memtable (&optional (id 0))
  "Create a new empty memtable."
  (%make-memtable :skiplist (make-skip-list) :id id))

(defun memtable-put (mt key value)
  "Put key-value pair into memtable. Returns previous value if any."
  (when (memtable-frozen-p mt)
    (error 'lsm-error :message "Cannot write to frozen memtable"))
  (let ((seq (incf (memtable-sequence-number mt))))
    (skip-list-insert (memtable-skiplist mt) key value seq)))

(defun memtable-get (mt key)
  "Get value for key. Returns (values value found-p deleted-p)."
  (multiple-value-bind (node update)
      (skip-list-find-path (memtable-skiplist mt) key)
    (declare (ignore update))
    (let ((cmp (skip-list-comparator (memtable-skiplist mt))))
      (if (and node (zerop (funcall cmp (skip-node-key node) key)))
          (values (skip-node-value node) t (skip-node-deleted-p node))
          (values nil nil nil)))))

(defun memtable-delete (mt key)
  "Delete key from memtable (adds tombstone)."
  (when (memtable-frozen-p mt)
    (error 'lsm-error :message "Cannot write to frozen memtable"))
  (let ((seq (incf (memtable-sequence-number mt))))
    (skip-list-delete (memtable-skiplist mt) key seq)))

(defun memtable-size (mt)
  "Return approximate size of memtable in bytes."
  (skip-list-bytes (memtable-skiplist mt)))

(defun memtable-count (mt)
  "Return number of entries in memtable."
  (skip-list-size (memtable-skiplist mt)))

(defun memtable-freeze (mt)
  "Freeze memtable for flushing. No more writes allowed."
  (setf (memtable-frozen-p mt) t))

(defun memtable-iterator (mt &optional start-key)
  "Create iterator over memtable entries."
  (skip-list-iterator (memtable-skiplist mt) start-key))

;;; end of memtable.lisp
