;;;; cl-lsm-tree.asd - System Definition
;;;;
;;;; Pure Common Lisp LSM-tree key-value storage engine.
;;;; Zero external dependencies - uses only SBCL built-ins.

(defsystem #:cl-lsm-tree
  :name "cl-lsm-tree"
  :version "1.0.0"
  :author "Parkian Company LLC"
  :license "MIT"
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
                :serial t
                :components ((:file "util")
                             (:file "memtable")
                             (:file "sstable")
                             (:file "compaction")
                             (:file "lsm"))))
  :in-order-to ((test-op (test-op #:cl-lsm-tree/test))))

(defsystem #:cl-lsm-tree/test
  :depends-on (#:cl-lsm-tree)
  :serial t
  :components ((:module "test"
                :components ((:file "test-lsm"))))
  :perform (test-op (o c)
             (uiop:symbol-call :cl-lsm-tree/test :run-tests)))
