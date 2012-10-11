(defpackage :in-mem-str
  (:use :cl)
  (:export :node :make-node
           :node-id :node-lon :node-lat :node-tags
           :node-blob-num :node-blob-elt :node-tags-st
           :node-offs-in-blob :node-pb-size

           :way :make-way
           :way-id :way-refs :way-tags :way-tags-st
           :way-blob-num :way-offs-in-blob :way-pb-size

           :make-index-arr

           :rel-member :make-rel-member
           :rel-member-id :rel-member-role :rel-member-type

           :relation :make-relation
           :relation-id :relation-tags :relation-tags-st
           :relation-members
           :relation-blob-num :relation-offs-in-blob :relation-pb-size
           :relation-mem-roles-st

           :make-bbox
           :bbox-min-lon :bbox-min-lat
           :bbox-max-lon :bbox-max-lat
           :make-bbox-leaf))

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

(defstruct way
  (id 0 :type (unsigned-byte 64))
  (refs (make-array 0 :element-type '(unsigned-byte 64)) :type (simple-array (unsigned-byte 64) (*)))
  (tags nil :type list)
  (tags-st nil :type list)
  (blob-num 0 :type integer)
  (offs-in-blob 0 :type (unsigned-byte 64))
  (pb-size 0 :type (unsigned-byte 64)))

(defstruct rel-member
  (id 0 :type (unsigned-byte 64))
  (role "" :type string)
  (type 0 :type (unsigned-byte 8)))

(defstruct relation
  (id 0 :type (unsigned-byte 64))
  (tags nil :type list)
  (tags-st nil :type list)
  (members (make-array 0 :element-type 'rel-member :initial-element (make-rel-member)) :type (simple-array rel-member (*)))
  (mem-roles-st nil :type list)
  (blob-num 0 :type integer)
  (offs-in-blob 0 :type (unsigned-byte 64))
  (pb-size 0 :type (unsigned-byte 64)))

(defstruct bbox
  (min-lon 3600000000 :type (unsigned-byte 64))
  (min-lat 3600000000 :type (unsigned-byte 64))
  (max-lon 0 :type (unsigned-byte 64))
  (max-lat 0 :type (unsigned-byte 64)))

(defmacro make-index-arr-any (val type)
  (let ((blob-index (gensym))
        (size (gensym))
        (buf (gensym))
        (type (string-upcase (if (typep type 'symbol) (symbol-name type) type))))
    `(let ((,blob-index (make-instance 'btreepbf:blob-index)))
       (setf (btreepbf:blob-num ,blob-index) (,(intern (format nil "~A-BLOB-NUM" type)) ,val)
             (btreepbf:blob-offs ,blob-index) (,(intern (format nil "~A-OFFS-IN-BLOB" type)) ,val)
             (btreepbf:size ,blob-index) (,(intern (format nil "~A-PB-SIZE" type)) ,val))
       (let* ((,size (pb:octet-size ,blob-index))
              (,buf (make-array ,size :element-type '(unsigned-byte 8))))
         (pb:serialize ,blob-index ,buf 0 ,size)
         ,buf))))

(defun make-node-index-arr (node)
  (make-index-arr-any node :node))

(defun make-way-index-arr (way)
  (make-index-arr-any way :way))

(defun make-relation-index-arr (relation)
  (make-index-arr-any relation :relation))

(defgeneric make-index-arr (item)
  (:method ((item node))
    (make-node-index-arr item))
  (:method ((item way))
    (make-way-index-arr item))
  (:method ((item relation))
    (make-relation-index-arr item)))

(defun make-bbox-leaf (bbox)
  (declare (type bbox bbox))
  (let ((pbbox (make-instance 'btreepbf:bbox)))
    (setf (btreepbf:min-lon pbbox) (bbox-min-lon bbox)
          (btreepbf:min-lat pbbox) (bbox-min-lat bbox)
          (btreepbf:max-lon pbbox) (- (bbox-max-lon bbox) (bbox-min-lon bbox))
          (btreepbf:max-lat pbbox) (- (bbox-max-lat bbox) (bbox-min-lat bbox)))
    (let* ((size (pb:octet-size pbbox))
           (buf (make-array size :element-type '(unsigned-byte 8))))
      (pb:serialize pbbox buf 0 size)
      buf)))
