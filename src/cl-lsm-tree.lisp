(in-package #:cl-lsm-tree)
(defvar *state* (make-hash-table :test 'equal))
(defvar *lock* (bt:make-lock))

(defun initialize ()
  (bt:with-lock-held (*lock*)
    (setf (gethash "status" *state*) :ready)
    (setf (gethash "started-at" *state*) (get-universal-time))
    (format t "cl-lsm-tree Service Initialized.
")
    t))

(defun shutdown ()
  (bt:with-lock-held (*lock*)
    (setf (gethash "status" *state*) :off)
    t))

(defun execute-request (op &rest params)
  (format t "[~A] Request: ~A with ~A~%" op params)
  (alexandria:plist-hash-table (list :result :success :op op :timestamp (get-universal-time))))

(defun get-status ()
  (bt:with-lock-held (*lock*)
    (gethash "status" *state*)))