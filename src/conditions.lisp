;; Copyright (c) 2024-2026 Parkian Company LLC. All rights reserved.
;; SPDX-License-Identifier: Apache-2.0

(in-package #:cl-lsm-tree)

(define-condition cl-lsm-tree-error (error)
  ((message :initarg :message :reader cl-lsm-tree-error-message))
  (:report (lambda (condition stream)
             (format stream "cl-lsm-tree error: ~A" (cl-lsm-tree-error-message condition)))))
