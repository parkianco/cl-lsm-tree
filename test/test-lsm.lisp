;;;; test-lsm.lisp - Tests for cl-lsm-tree
;;;;
;;;; Basic test suite for LSM-tree storage engine.

(defpackage #:cl-lsm-tree/test
  (:use #:cl #:cl-lsm-tree)
  (:export #:run-tests))

(in-package #:cl-lsm-tree/test)

;;; ============================================================================
;;; Test Infrastructure
;;; ============================================================================

(defvar *test-count* 0)
(defvar *pass-count* 0)
(defvar *fail-count* 0)

(defmacro deftest (name &body body)
  "Define a test case."
  `(defun ,name ()
     (incf *test-count*)
     (handler-case
         (progn
           ,@body
           (incf *pass-count*)
           (format t "  PASS: ~A~%" ',name)
           t)
       (error (e)
         (incf *fail-count*)
         (format t "  FAIL: ~A - ~A~%" ',name e)
         nil))))

(defmacro assert-equal (expected actual &optional message)
  "Assert that expected equals actual."
  `(unless (equal ,expected ,actual)
     (error "~@[~A: ~]Expected ~S but got ~S"
            ,message ,expected ,actual)))

(defmacro assert-true (expr &optional message)
  "Assert that expression is true."
  `(unless ,expr
     (error "~@[~A: ~]Expected true but got ~S" ,message ,expr)))

(defmacro assert-nil (expr &optional message)
  "Assert that expression is NIL."
  `(when ,expr
     (error "~@[~A: ~]Expected NIL but got ~S" ,message ,expr)))

(defun temp-directory ()
  "Create a temporary directory for tests."
  (let ((dir (merge-pathnames
              (format nil "lsm-test-~A/" (get-universal-time))
              (uiop:temporary-directory))))
    (ensure-directories-exist dir)
    dir))

(defun cleanup-directory (dir)
  "Remove test directory and contents."
  (when (probe-file dir)
    (dolist (file (directory (merge-pathnames "*.*" dir)))
      (ignore-errors (delete-file file)))
    (ignore-errors (uiop:delete-directory-tree dir :validate t))))

;;; ============================================================================
;;; Skip List Tests
;;; ============================================================================

(deftest test-skip-list-basic
  (let ((sl (cl-lsm-tree::make-skip-list)))
    (cl-lsm-tree::skip-list-insert sl "key1" "value1" 1)
    (cl-lsm-tree::skip-list-insert sl "key2" "value2" 2)
    (multiple-value-bind (val found)
        (cl-lsm-tree::skip-list-get sl "key1")
      (assert-true found "key1 should be found")
      (assert-equal "value1" val "key1 value"))))

(deftest test-skip-list-update
  (let ((sl (cl-lsm-tree::make-skip-list)))
    (cl-lsm-tree::skip-list-insert sl "key1" "value1" 1)
    (cl-lsm-tree::skip-list-insert sl "key1" "value2" 2)
    (multiple-value-bind (val found)
        (cl-lsm-tree::skip-list-get sl "key1")
      (assert-true found)
      (assert-equal "value2" val "updated value"))))

(deftest test-skip-list-delete
  (let ((sl (cl-lsm-tree::make-skip-list)))
    (cl-lsm-tree::skip-list-insert sl "key1" "value1" 1)
    (cl-lsm-tree::skip-list-delete sl "key1" 2)
    (multiple-value-bind (val found)
        (cl-lsm-tree::skip-list-get sl "key1")
      (declare (ignore val))
      (assert-nil found "deleted key should not be found"))))

;;; ============================================================================
;;; Bloom Filter Tests
;;; ============================================================================

(deftest test-bloom-filter-basic
  (let ((bf (cl-lsm-tree::make-bloom-filter 100)))
    (cl-lsm-tree::bloom-add bf "key1")
    (cl-lsm-tree::bloom-add bf "key2")
    (assert-true (cl-lsm-tree::bloom-may-contain bf "key1"))
    (assert-true (cl-lsm-tree::bloom-may-contain bf "key2"))))

(deftest test-bloom-filter-serialize
  (let ((bf (cl-lsm-tree::make-bloom-filter 100)))
    (cl-lsm-tree::bloom-add bf "key1")
    (let* ((bytes (cl-lsm-tree::bloom-serialize bf))
           (bf2 (cl-lsm-tree::bloom-deserialize bytes)))
      (assert-true (cl-lsm-tree::bloom-may-contain bf2 "key1")))))

;;; ============================================================================
;;; Block Cache Tests
;;; ============================================================================

(deftest test-cache-basic
  (let ((cache (cl-lsm-tree::make-block-cache 1024)))
    (cl-lsm-tree::cache-put cache "key1" "data1" 100)
    (assert-equal "data1" (cl-lsm-tree::cache-get cache "key1"))))

(deftest test-cache-eviction
  (let ((cache (cl-lsm-tree::make-block-cache 200)))
    (cl-lsm-tree::cache-put cache "key1" "data1" 100)
    (cl-lsm-tree::cache-put cache "key2" "data2" 100)
    (cl-lsm-tree::cache-put cache "key3" "data3" 100)
    ;; key1 should be evicted
    (assert-nil (cl-lsm-tree::cache-get cache "key1"))))

;;; ============================================================================
;;; MemTable Tests
;;; ============================================================================

(deftest test-memtable-basic
  (let ((mt (cl-lsm-tree::make-memtable)))
    (cl-lsm-tree::memtable-put mt "key1" "value1")
    (multiple-value-bind (val found deleted)
        (cl-lsm-tree::memtable-get mt "key1")
      (declare (ignore deleted))
      (assert-true found)
      (assert-equal "value1" val))))

(deftest test-memtable-freeze
  (let ((mt (cl-lsm-tree::make-memtable)))
    (cl-lsm-tree::memtable-put mt "key1" "value1")
    (cl-lsm-tree::memtable-freeze mt)
    (handler-case
        (progn
          (cl-lsm-tree::memtable-put mt "key2" "value2")
          (error "Should have signaled error"))
      (lsm-error ()
        t))))

;;; ============================================================================
;;; Engine Tests
;;; ============================================================================

(deftest test-engine-basic
  (let ((dir (temp-directory)))
    (unwind-protect
         (with-engine (engine dir)
           (engine-put engine "hello" "world")
           (multiple-value-bind (val found)
               (engine-get engine "hello")
             (assert-true found)
             (assert-equal (sb-ext:string-to-octets "world" :external-format :utf-8)
                           val)))
      (cleanup-directory dir))))

(deftest test-engine-delete
  (let ((dir (temp-directory)))
    (unwind-protect
         (with-engine (engine dir)
           (engine-put engine "key" "value")
           (engine-delete engine "key")
           (multiple-value-bind (val found)
               (engine-get engine "key")
             (declare (ignore val))
             (assert-nil found)))
      (cleanup-directory dir))))

(deftest test-engine-persistence
  (let ((dir (temp-directory)))
    (unwind-protect
         (progn
           ;; Write data
           (with-engine (engine dir)
             (engine-put engine "persist-key" "persist-value"))
           ;; Read back
           (with-engine (engine dir)
             (multiple-value-bind (val found)
                 (engine-get engine "persist-key")
               (assert-true found)
               (assert-equal (sb-ext:string-to-octets "persist-value"
                                                      :external-format :utf-8)
                             val))))
      (cleanup-directory dir))))

(deftest test-engine-scan
  (let ((dir (temp-directory)))
    (unwind-protect
         (with-engine (engine dir)
           (engine-put engine "a" "1")
           (engine-put engine "b" "2")
           (engine-put engine "c" "3")
           (let ((results (engine-scan engine)))
             (assert-equal 3 (length results))))
      (cleanup-directory dir))))

(deftest test-engine-batch
  (let ((dir (temp-directory)))
    (unwind-protect
         (with-engine (engine dir)
           (let ((batch (make-write-batch)))
             (batch-put batch "k1" "v1")
             (batch-put batch "k2" "v2")
             (batch-apply engine batch))
           (assert-true (engine-contains-p engine "k1"))
           (assert-true (engine-contains-p engine "k2")))
      (cleanup-directory dir))))

(deftest test-engine-snapshot
  (let ((dir (temp-directory)))
    (unwind-protect
         (with-engine (engine dir)
           (engine-put engine "snap-key" "value1")
           (let ((snap (create-snapshot engine)))
             ;; Modify after snapshot
             (engine-put engine "snap-key" "value2")
             ;; Snapshot should see old value
             (multiple-value-bind (val found)
                 (snapshot-get snap "snap-key")
               (assert-true found)
               (assert-equal (sb-ext:string-to-octets "value1"
                                                      :external-format :utf-8)
                             val))
             (release-snapshot engine snap)))
      (cleanup-directory dir))))

(deftest test-engine-statistics
  (let ((dir (temp-directory)))
    (unwind-protect
         (with-engine (engine dir)
           (engine-put engine "key" "value")
           (engine-get engine "key")
           (let ((stats (engine-statistics engine)))
             (assert-true (>= (getf stats :writes) 1))
             (assert-true (>= (getf stats :reads) 1))))
      (cleanup-directory dir))))

;;; ============================================================================
;;; Run All Tests
;;; ============================================================================

(defun run-tests ()
  "Run all tests and report results."
  (setf *test-count* 0
        *pass-count* 0
        *fail-count* 0)
  (format t "~%Running cl-lsm-tree tests...~%~%")

  ;; Skip list tests
  (format t "Skip List Tests:~%")
  (test-skip-list-basic)
  (test-skip-list-update)
  (test-skip-list-delete)

  ;; Bloom filter tests
  (format t "~%Bloom Filter Tests:~%")
  (test-bloom-filter-basic)
  (test-bloom-filter-serialize)

  ;; Cache tests
  (format t "~%Block Cache Tests:~%")
  (test-cache-basic)
  (test-cache-eviction)

  ;; MemTable tests
  (format t "~%MemTable Tests:~%")
  (test-memtable-basic)
  (test-memtable-freeze)

  ;; Engine tests
  (format t "~%Engine Tests:~%")
  (test-engine-basic)
  (test-engine-delete)
  (test-engine-persistence)
  (test-engine-scan)
  (test-engine-batch)
  (test-engine-snapshot)
  (test-engine-statistics)

  ;; Summary
  (format t "~%==============================~%")
  (format t "Tests: ~D  Passed: ~D  Failed: ~D~%"
          *test-count* *pass-count* *fail-count*)
  (format t "==============================~%")

  (zerop *fail-count*))

;;; end of test-lsm.lisp
