;; Copyright (c) 2024-2026 Parkian Company LLC. All rights reserved.
;; SPDX-License-Identifier: BSD-3-Clause

;;;; cl-lsm-tree.asd - System Definition
;;;;
;;;; Pure Common Lisp LSM-tree key-value storage engine.
;;;; Zero external dependencies - uses only SBCL built-ins.

(asdf:defsystem #:cl-lsm-tree
  :name "cl-lsm-tree"
  :version "0.1.0"
  :author "Park Ian Co"
  :license "Apache-2.0"
  :description "Log-Structured Merge-tree key-value store in pure Common Lisp"
  :long-description "A production-ready LSM-tree storage engine implementing:
- MemTable with skip list for in-memory writes
- SSTable format with block-based layout for persistent storage
- Write-Ahead Log (WAL) for durability and crash recovery
- Compaction strategies (leveled, tiered, FIFO)
- Bloom filters for efficient negative lookups
- Block cache with LRU eviction
- Range scan iterators with merge semantics
- Snapshot isolation for consistent reads"
  :serial t
  :components ((:file "package")
               (:module "src"
                :components ((:file "package")
                             (:file "conditions" :depends-on ("package"))
                             (:file "types" :depends-on ("package"))
                             (:file "cl-lsm-tree" :depends-on ("package" "conditions" "types"))))))
  :in-order-to ((asdf:test-op (test-op #:cl-lsm-tree/test))))

(asdf:defsystem #:cl-lsm-tree/test
  :depends-on (#:cl-lsm-tree)
  :serial t
  :components ((:module "test"
                :components ((:file "test-lsm"))))
  :perform (asdf:test-op (o c)
             (let ((result (uiop:symbol-call :cl-lsm-tree/test :run-tests)))
               (unless result
                 (error "Tests failed")))))
