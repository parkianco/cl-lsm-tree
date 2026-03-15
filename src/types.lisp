;; Copyright (c) 2024-2026 Parkian Company LLC. All rights reserved.
;; SPDX-License-Identifier: Apache-2.0

(in-package #:cl-lsm-tree)

;;; Core types for cl-lsm-tree
(deftype cl-lsm-tree-id () '(unsigned-byte 64))
(deftype cl-lsm-tree-status () '(member :ready :active :error :shutdown))
