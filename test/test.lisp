;; Copyright (c) 2024-2026 Parkian Company LLC. All rights reserved.
;; SPDX-License-Identifier: Apache-2.0

(defpackage #:cl-lsm-tree.test
  (:use #:cl #:cl-lsm-tree)
  (:export #:run-tests))

(in-package #:cl-lsm-tree.test)

(defun run-tests ()
  (format t "Executing functional test suite for cl-lsm-tree...~%")
  (assert (equal (deep-copy-list '(1 (2 3) 4)) '(1 (2 3) 4)))
  (assert (equal (group-by-count '(1 2 3 4 5) 2) '((1 2) (3 4) (5))))
  (format t "All functional tests passed!~%")
  t
  ;; Layer 2 Tests
  (let ((m-fn (memoize-function (lambda (x) (* x 2))))) (assert (= (funcall m-fn 5) 10)) (assert (= (funcall m-fn 5) 10)))
  t)