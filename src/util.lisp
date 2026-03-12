;;;; Copyright (c) 2024-2026 Parkian Company LLC. All rights reserved.
;;;; SPDX-License-Identifier: BSD-3-Clause

;;;; util.lisp - Utility functions and constants
;;;;
;;;; Common utilities for LSM-tree implementation.

(in-package #:cl-lsm-tree)

;;; ============================================================================
;;; Constants
;;; ============================================================================

(defconstant +lsm-magic+ #x4C534D54  ; "LSMT"
  "Magic number for LSM engine files.")

(defconstant +sstable-magic+ #x53535442  ; "SSTB"
  "Magic number for SSTable files.")

(defconstant +block-size+ 4096
  "Default block size: 4KB.")

(defconstant +memtable-size-threshold+ (* 64 1024 1024)
  "MemTable flush threshold: 64MB.")

(defconstant +max-skip-list-height+ 32
  "Maximum height for skip list nodes.")

(defconstant +skip-list-probability+ 0.25
  "Probability for skip list level promotion.")

(defconstant +max-levels+ 7
  "Maximum number of LSM levels.")

(defconstant +level-size-ratio+ 10
  "Size ratio between consecutive levels.")

(defconstant +bloom-bits-per-key+ 10
  "Bits per key for bloom filter.")

(defconstant +restart-interval+ 16
  "Restart interval for block prefix compression.")

(defconstant +tombstone-marker+ #xDEAD
  "Marker for deleted entries.")

;;; ============================================================================
;;; Conditions
;;; ============================================================================

(define-condition lsm-error (error)
  ((message :initarg :message :reader lsm-error-message)
   (path :initarg :path :reader lsm-error-path :initform nil))
  (:report (lambda (c s)
             (format s "LSM error~@[ at ~A~]: ~A"
                     (lsm-error-path c) (lsm-error-message c)))))

(define-condition lsm-corruption-error (lsm-error)
  ((offset :initarg :offset :reader corruption-offset))
  (:report (lambda (c s)
             (format s "LSM corruption at offset ~A: ~A"
                     (corruption-offset c) (lsm-error-message c)))))

(define-condition lsm-io-error (lsm-error)
  ((operation :initarg :operation :reader io-operation))
  (:report (lambda (c s)
             (format s "LSM I/O error during ~A: ~A"
                     (io-operation c) (lsm-error-message c)))))

;;; ============================================================================
;;; Key Comparison
;;; ============================================================================

(defun key-compare (a b)
  "Compare two keys lexicographically.
   Returns -1, 0, or 1."
  (cond
    ((null a) -1)
    ((null b) 1)
    ((equal a b) 0)
    ((and (stringp a) (stringp b))
     (cond ((string< a b) -1)
           ((string> a b) 1)
           (t 0)))
    ((and (vectorp a) (vectorp b))
     (let ((len-a (length a))
           (len-b (length b)))
       (loop for i below (min len-a len-b)
             for byte-a = (aref a i)
             for byte-b = (aref b i)
             when (< byte-a byte-b) return -1
             when (> byte-a byte-b) return 1
             finally (return (cond ((< len-a len-b) -1)
                                   ((> len-a len-b) 1)
                                   (t 0))))))
    (t 0)))

;;; ============================================================================
;;; Bloom Filter
;;; ============================================================================

(defstruct (bloom-filter (:constructor %make-bloom-filter))
  "Space-efficient probabilistic data structure for membership testing.

   False positives possible, false negatives impossible.
   Used to avoid unnecessary disk reads for non-existent keys."
  (bits nil :type simple-bit-vector)
  (num-bits 0 :type fixnum)
  (num-hashes 0 :type fixnum)
  (num-keys 0 :type fixnum))

(defun make-bloom-filter (expected-keys &optional (bits-per-key +bloom-bits-per-key+))
  "Create bloom filter sized for expected number of keys."
  (let* ((num-bits (max 64 (* expected-keys bits-per-key)))
         (num-hashes (max 1 (round (* 0.693 bits-per-key)))))
    (%make-bloom-filter
     :bits (make-array num-bits :element-type 'bit :initial-element 0)
     :num-bits num-bits
     :num-hashes num-hashes)))

(defun bloom-hash (key seed)
  "Compute hash of key with given seed using MurmurHash-like mixing."
  (let ((h (if seed seed #x5BD1E995)))
    (declare (type (unsigned-byte 32) h))
    (etypecase key
      ((simple-array (unsigned-byte 8) (*))
       (loop for byte across key
             do (setf h (logxor h byte))
                (setf h (logand #xFFFFFFFF (* h #x5BD1E995)))
                (setf h (logxor h (ash h -15)))))
      (string
       (loop for char across key
             do (setf h (logxor h (char-code char)))
                (setf h (logand #xFFFFFFFF (* h #x5BD1E995)))
                (setf h (logxor h (ash h -15))))))
    (logand #xFFFFFFFF (logxor h (ash h -13)))))

(defun bloom-add (bf key)
  "Add key to bloom filter."
  (let* ((h1 (bloom-hash key nil))
         (h2 (bloom-hash key h1))
         (n (bloom-filter-num-bits bf))
         (bits (bloom-filter-bits bf)))
    (loop for i below (bloom-filter-num-hashes bf)
          for pos = (mod (+ h1 (* i h2)) n)
          do (setf (sbit bits pos) 1))
    (incf (bloom-filter-num-keys bf))))

(defun bloom-may-contain (bf key)
  "Check if key may be in filter. Returns T if possibly present."
  (let* ((h1 (bloom-hash key nil))
         (h2 (bloom-hash key h1))
         (n (bloom-filter-num-bits bf))
         (bits (bloom-filter-bits bf)))
    (loop for i below (bloom-filter-num-hashes bf)
          for pos = (mod (+ h1 (* i h2)) n)
          always (= 1 (sbit bits pos)))))

(defun bloom-serialize (bf)
  "Serialize bloom filter to byte vector."
  (let* ((byte-count (ceiling (bloom-filter-num-bits bf) 8))
         (bytes (make-array (+ 12 byte-count) :element-type '(unsigned-byte 8))))
    ;; Header: num-bits (4), num-hashes (4), num-keys (4)
    (setf (aref bytes 0) (ldb (byte 8 0) (bloom-filter-num-bits bf))
          (aref bytes 1) (ldb (byte 8 8) (bloom-filter-num-bits bf))
          (aref bytes 2) (ldb (byte 8 16) (bloom-filter-num-bits bf))
          (aref bytes 3) (ldb (byte 8 24) (bloom-filter-num-bits bf))
          (aref bytes 4) (ldb (byte 8 0) (bloom-filter-num-hashes bf))
          (aref bytes 5) (ldb (byte 8 8) (bloom-filter-num-hashes bf))
          (aref bytes 6) (ldb (byte 8 16) (bloom-filter-num-hashes bf))
          (aref bytes 7) (ldb (byte 8 24) (bloom-filter-num-hashes bf))
          (aref bytes 8) (ldb (byte 8 0) (bloom-filter-num-keys bf))
          (aref bytes 9) (ldb (byte 8 8) (bloom-filter-num-keys bf))
          (aref bytes 10) (ldb (byte 8 16) (bloom-filter-num-keys bf))
          (aref bytes 11) (ldb (byte 8 24) (bloom-filter-num-keys bf)))
    ;; Bit data
    (loop for i below byte-count
          do (loop for j below 8
                   for bit-idx = (+ (* i 8) j)
                   when (< bit-idx (bloom-filter-num-bits bf))
                     do (setf (aref bytes (+ 12 i))
                              (dpb (sbit (bloom-filter-bits bf) bit-idx)
                                   (byte 1 j)
                                   (aref bytes (+ 12 i))))))
    bytes))

(defun bloom-deserialize (bytes)
  "Deserialize bloom filter from byte vector."
  (let* ((num-bits (+ (aref bytes 0)
                      (ash (aref bytes 1) 8)
                      (ash (aref bytes 2) 16)
                      (ash (aref bytes 3) 24)))
         (num-hashes (+ (aref bytes 4)
                        (ash (aref bytes 5) 8)
                        (ash (aref bytes 6) 16)
                        (ash (aref bytes 7) 24)))
         (num-keys (+ (aref bytes 8)
                      (ash (aref bytes 9) 8)
                      (ash (aref bytes 10) 16)
                      (ash (aref bytes 11) 24)))
         (bits (make-array num-bits :element-type 'bit :initial-element 0)))
    (loop for i below (ceiling num-bits 8)
          do (loop for j below 8
                   for bit-idx = (+ (* i 8) j)
                   when (< bit-idx num-bits)
                     do (setf (sbit bits bit-idx)
                              (ldb (byte 1 j) (aref bytes (+ 12 i))))))
    (%make-bloom-filter
     :bits bits
     :num-bits num-bits
     :num-hashes num-hashes
     :num-keys num-keys)))

;;; ============================================================================
;;; Block Cache (LRU)
;;; ============================================================================

(defstruct (cache-entry (:constructor make-cache-entry (key data size)))
  "Entry in block cache."
  (key nil :type t)
  (data nil :type t)
  (size 0 :type fixnum)
  (prev nil :type (or null cache-entry))
  (next nil :type (or null cache-entry)))

(defstruct (block-cache (:constructor %make-block-cache))
  "LRU cache for SSTable data blocks.

   Reduces disk I/O by caching frequently accessed blocks."
  (capacity 0 :type fixnum)
  (size 0 :type fixnum)
  (table (make-hash-table :test 'equal) :type hash-table)
  (head nil :type (or null cache-entry))
  (tail nil :type (or null cache-entry))
  (hits 0 :type fixnum)
  (misses 0 :type fixnum)
  (lock (sb-thread:make-mutex :name "block-cache") :type t))

(defun make-block-cache (&optional (capacity (* 256 1024 1024)))
  "Create block cache with specified capacity in bytes."
  (%make-block-cache :capacity capacity))

(defun cache-move-to-head (cache entry)
  "Move entry to head of LRU list."
  ;; Remove from current position
  (when (cache-entry-prev entry)
    (setf (cache-entry-next (cache-entry-prev entry))
          (cache-entry-next entry)))
  (when (cache-entry-next entry)
    (setf (cache-entry-prev (cache-entry-next entry))
          (cache-entry-prev entry)))
  (when (eq entry (block-cache-tail cache))
    (setf (block-cache-tail cache) (cache-entry-prev entry)))
  ;; Add to head
  (setf (cache-entry-prev entry) nil)
  (setf (cache-entry-next entry) (block-cache-head cache))
  (when (block-cache-head cache)
    (setf (cache-entry-prev (block-cache-head cache)) entry))
  (setf (block-cache-head cache) entry)
  (unless (block-cache-tail cache)
    (setf (block-cache-tail cache) entry)))

(defun cache-evict-lru (cache bytes-needed)
  "Evict LRU entries until bytes-needed is available."
  (loop while (and (block-cache-tail cache)
                   (> (+ (block-cache-size cache) bytes-needed)
                      (block-cache-capacity cache)))
        do (let ((victim (block-cache-tail cache)))
             ;; Remove from list
             (setf (block-cache-tail cache) (cache-entry-prev victim))
             (when (block-cache-tail cache)
               (setf (cache-entry-next (block-cache-tail cache)) nil))
             (when (eq victim (block-cache-head cache))
               (setf (block-cache-head cache) nil))
             ;; Remove from table
             (remhash (cache-entry-key victim) (block-cache-table cache))
             (decf (block-cache-size cache) (cache-entry-size victim)))))

(defun cache-get (cache key)
  "Get data from cache. Returns data or NIL."
  (sb-thread:with-mutex ((block-cache-lock cache))
    (let ((entry (gethash key (block-cache-table cache))))
      (cond
        (entry
         (incf (block-cache-hits cache))
         (cache-move-to-head cache entry)
         (cache-entry-data entry))
        (t
         (incf (block-cache-misses cache))
         nil)))))

(defun cache-put (cache key data size)
  "Put data into cache, evicting if necessary."
  (sb-thread:with-mutex ((block-cache-lock cache))
    (let ((existing (gethash key (block-cache-table cache))))
      (if existing
          ;; Update existing
          (progn
            (decf (block-cache-size cache) (cache-entry-size existing))
            (setf (cache-entry-data existing) data
                  (cache-entry-size existing) size)
            (incf (block-cache-size cache) size)
            (cache-move-to-head cache existing))
          ;; Insert new
          (progn
            (cache-evict-lru cache size)
            (let ((entry (make-cache-entry key data size)))
              (setf (gethash key (block-cache-table cache)) entry)
              (incf (block-cache-size cache) size)
              (cache-move-to-head cache entry)))))))

(defun cache-hit-rate (cache)
  "Calculate cache hit rate."
  (let ((total (+ (block-cache-hits cache) (block-cache-misses cache))))
    (if (zerop total) 0.0 (/ (block-cache-hits cache) total))))

;;; end of util.lisp
