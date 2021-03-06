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

           :bbox 
           :make-bbox :copy-bbox :copy-to-bbox :extend-bbox
           :bbox-area :bbox-margin :bbox-overlap
           :bbox-min-lon :bbox-min-lat
           :bbox-max-lon :bbox-max-lat
           :make-bbox-leaf
           :bbox-from-list))

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

(defun copy-to-bbox (dst src)
  (setf (bbox-min-lon dst) (bbox-min-lon src)
        (bbox-min-lat dst) (bbox-min-lat src)
        (bbox-max-lon dst) (bbox-max-lon src)
        (bbox-max-lat dst) (bbox-max-lat src)))

(defun extend-bbox (dst src)
  (setf (bbox-min-lon dst) (min (bbox-min-lon dst) (bbox-min-lon src))
        (bbox-min-lat dst) (min (bbox-min-lat dst) (bbox-min-lat src))
        (bbox-max-lon dst) (max (bbox-max-lon dst) (bbox-max-lon src))
        (bbox-max-lat dst) (max (bbox-max-lat dst) (bbox-max-lat src)))
  dst)

(defun bbox-area (bbox)
  (* (- (bbox-max-lon bbox) (bbox-min-lon bbox)) (- (bbox-max-lat bbox) (bbox-min-lat bbox))))

(defun bbox-margin (bbox)
  (* 2 (+ (- (bbox-max-lon bbox) (bbox-min-lon bbox)) (- (bbox-max-lat bbox) (bbox-min-lat bbox)))))

(defun bbox-overlap (bbox1 bbox2)
  "Calculate the overlap of bboxes"
  (let ((res (make-bbox
              :min-lon (max (bbox-min-lon bbox1) (bbox-min-lon bbox2))
              :min-lat (max (bbox-min-lat bbox1) (bbox-min-lat bbox2))
              :max-lon (min (bbox-max-lon bbox1) (bbox-max-lon bbox2))
              :max-lat (min (bbox-max-lat bbox1) (bbox-max-lat bbox2)))))
    (unless (or (> (bbox-min-lon res) (bbox-max-lon res))
                (> (bbox-min-lat res) (bbox-max-lat res)))
      res)))

(defun bbox-from-list (data)
  "convert (list min-lon min-lat max-lon max-lat) to bbox structure"
  (declare (type list data))
  (make-bbox :min-lon (first data)
             :min-lat (second data)
             :max-lon (third data)
             :max-lat (fourth data)))

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
