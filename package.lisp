;;;; Copyright (c) 2024-2026 Parkian Company LLC. All rights reserved.
;;;; SPDX-License-Identifier: Apache-2.0

;;;; package.lisp - Package definitions for cl-lsm-tree
;;;;
;;;; Pure Common Lisp LSM-tree storage engine - zero external dependencies.

(defpackage #:cl-lsm-tree
  (:use #:cl)
  (:nicknames #:lsm-tree #:lsm)
  (:documentation "Log-Structured Merge-tree key-value storage engine.

A production-ready LSM-tree implementation with:
- O(log n) write and read operations
- Durability via Write-Ahead Log
- Background compaction
- Snapshot isolation for consistent reads
- Bloom filters for efficient negative lookups
- LRU block cache for read optimization

BASIC USAGE:
  (with-engine (engine \"/path/to/data\")
    (engine-put engine \"key\" \"value\")
    (engine-get engine \"key\"))

BATCH WRITES:
  (let ((batch (make-write-batch)))
    (batch-put batch \"key1\" \"value1\")
    (batch-put batch \"key2\" \"value2\")
    (batch-apply engine batch))

SNAPSHOTS:
  (let ((snap (create-snapshot engine)))
    (snapshot-get snap \"key\")
    (release-snapshot engine snap))")
  (:export
   ;; Engine lifecycle
   #:lsm-engine
   #:make-lsm-engine
   #:engine-open
   #:engine-close
   #:engine-destroy
   #:with-engine

   ;; Core operations
   #:engine-get
   #:engine-put
   #:engine-delete
   #:engine-contains-p
   #:engine-scan
   #:engine-scan-range

   ;; Batch operations
   #:write-batch
   #:make-write-batch
   #:batch-put
   #:batch-delete
   #:batch-apply
   #:batch-clear

   ;; Snapshots
   #:create-snapshot
   #:release-snapshot
   #:snapshot-get
   #:snapshot-scan

   ;; Compaction
   #:trigger-compaction
   #:await-compaction
   #:set-compaction-strategy

   ;; Statistics
   #:engine-statistics
   #:memtable-size
   #:sstable-count
   #:cache-hit-rate

   ;; Conditions
   #:lsm-error
   #:lsm-error-message
   #:lsm-corruption-error
   #:lsm-io-error))
