(defpackage :in-mem-str
  (:use :cl)
  (:export :node :make-node
           :node-id :node-lon :node-lat :node-tags
           :node-blob-num :node-blob-elt :node-tags-st
           :node-offs-in-blob :node-pb-size
           :make-node-index-arr

           :way :make-way
           :way-id :way-refs :way-tags

           :rel-member :make-rel-member
           :rel-member-id :rel-member-role :rel-member-type

           :relation :make-relation
           :relation-id :relation-tags :relation-members)) 

(in-package :in-mem-str)

(defstruct node
  (id 0 :type (unsigned-byte 64))
  (lon 0 :type (unsigned-byte 64))
  (lat 0 :type (unsigned-byte 64))
  (tags nil :type list)
  (tags-st nil :type list)
  (blob-num 0 :type integer)
  (offs-in-blob 0 :type (unsigned-byte 64))
  (pb-size 0 :type (unsigned-byte 64)))

(defun make-node-index-arr (node)
  (let ((blob-index (make-instance 'btreepbf:blob-index)))
    (setf (btreepbf:blob-num blob-index) (node-blob-num node)
          (btreepbf:blob-offs blob-index) (node-offs-in-blob node)
          (btreepbf:size blob-index) (node-pb-size node))
    (let* ((size (pb:octet-size blob-index))
           (buf (make-array size :element-type '(unsigned-byte 8))))
      (pb:serialize blob-index buf 0 size)
      buf)))

(defstruct way
  (id 0 :type (unsigned-byte 64))
  (refs (make-array 0 :element-type '(unsigned-byte 64)) :type (simple-array (unsigned-byte 64) (*)))
  (tags nil :type list))

(defstruct rel-member
  (id 0 :type (unsigned-byte 64))
  (role "" :type string)
  (type 0 :type (unsigned-byte 8)))

(defstruct relation
  (id 0 :type (unsigned-byte 64))
  (tags nil :type list)
  (members (make-array 0 :element-type 'rel-member :initial-element (make-rel-member)) :type (simple-array rel-member (*))))
