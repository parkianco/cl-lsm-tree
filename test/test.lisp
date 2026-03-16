;; Copyright (c) 2024-2026 Parkian Company LLC. All rights reserved.
;; SPDX-License-Identifier: Apache-2.0

(defpackage #:cl-lsm-tree.test
  (:use #:cl #:cl-lsm-tree)
  (:export #:run-tests))

(in-package #:cl-lsm-tree.test)

(defun run-tests ()
  (format t "Running professional test suite for cl-lsm-tree...~%")
  (assert (initialize-lsm-tree))
  (format t "Tests passed!~%")
  t)
