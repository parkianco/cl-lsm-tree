(asdf:defsystem #:cl-lsm-tree
  :depends-on (#:alexandria #:bordeaux-threads)
  :components ((:module "src"
                :components ((:file "package")
                             (:file "cl-lsm-tree" :depends-on ("package"))))))