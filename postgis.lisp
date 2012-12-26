(defpackage :osm-postgis
  (:nicknames :db)
  (:use :cl
        :postmodern
        :s-sql
        :cl-postgres
        :b-tree
        :in-mem-str)
  (:export :db-connect
           :write-way
           :check-rel-members
           :write-rel
           :write-boundary
           :create-boundary-polys
           :write-building-rel
           :write-building-way
           :create-building-polys
           :query-point

           :update-stringtable
           :store-node
           :store-way
           :store-relation))

(in-package :osm-postgis)

(defvar *db-host* "localhost")
(defvar *db-name* "gis")
(defvar *db-user* "gis")
(defvar *db-password* "gis")

(defun db-connect (&key (host *db-host*) (name *db-name*) (user *db-user*) (password *db-password*))
  (when *database* (disconnect-toplevel))
  (connect-toplevel name user password host))

(defun make-way-linestring (way nodes-btree)
  (let ((points-list
         (loop for nid across (way-refs way)
            collect (let ((node (bsearch nodes-btree nid)))
                      (unless node (return-from make-way-linestring nil))
                      ;; FIXME! hardcoded offsets and granularity 
                      (format nil "~F ~F"
                              (* .000000001 100 (node-lon node))
                              (* .000000001 100 (node-lat node)))))))
    (when (< (length points-list) 2) (return-from make-way-linestring nil))
    (format nil "SRID=4326;LINESTRING(~A~{,~A~})" (car points-list) (cdr points-list))))

(defun write-way (way nodes-btree)
  (let ((ls (make-way-linestring way nodes-btree)))
    (when ls
      (execute
       (format nil "insert into way_geom values (~D, st_geomfromewkt(~A))" (way-id way) (sql-escape ls)))
      t)))


(defun check-rel-members (rel)
  "Check if all necessary relation memebers (only check for ways now) exist in database"
  (let ((found nil))
    (loop for mem across (relation-members rel)
       do
         (when (= (rel-member-type mem) osmpbf:+relation-member-type-way+)
           (if (query (:select 'id :from 'way-geom :where (:= 'id (rel-member-id mem))) :single)
               (setf found t)
               (return-from check-rel-members nil))))
    found))

;; FIXME: copypaste from osm-reader
(defun find-tag (tags key)
  (dolist (tag tags)
    (when (string= (car tag) key)
      (return-from find-tag tag)))
  nil)

(defun tag-value (tag def)
  (if tag
      (cdr tag)
      def))

(defun write-relation-ways (rel)
  (loop for mem across (relation-members rel)
         do
         (when (= (rel-member-type mem) osmpbf:+relation-member-type-way+)
           (execute (:insert-into 'relation-ways
                                  :set
                                  'rel-id (relation-id rel)
                                  'way-id (rel-member-id mem))))))

(defun write-boundary (rel)
  (with-transaction ()
    (execute (:insert-into 'boundary
                           :set
                           'id (relation-id rel)
                           'name (tag-value (find-tag (relation-tags rel) "name") :NULL)
                           'admin-level (let ((val (tag-value (find-tag (relation-tags rel) "admin_level") "-1")))
                                          (if (integerp (read-from-string val)) val -1))))
    (write-relation-ways rel)))

(defun write-building-rel (rel)
  (with-transaction ()
    (execute (:insert-into 'building
                           :set
                           'id (relation-id rel)
                           'name (tag-value (find-tag (relation-tags rel) "name") :NULL)
                           'street (tag-value (find-tag (relation-tags rel) "addr:street") :NULL)
                           'housenumber (tag-value (find-tag (relation-tags rel) "addr:housenumber") :NULL)
                           'is-rel t))
    (write-relation-ways rel)))

(defun write-building-way (way)
  (with-transaction ()
    (execute (:insert-into 'building
                           :set
                           'id (way-id way)
                           'name (tag-value (find-tag (way-tags way) "name") :NULL)
                           'street (tag-value (find-tag (way-tags way) "addr:street") :NULL)
                           'housenumber (tag-value (find-tag (way-tags way) "addr:housenumber") :NULL)))))

(defun query-point (lon lat)
  (let ((point-str (format nil "SRID=4326;POINT(~F ~F)" lon lat)))
    (list 
     (query (:order-by
             (:select '* :from 'boundary
                      :where (:in 'id (:select 'id
                                               :from 'boundary-poly
                                               :where (:st-within (:st-geomfromewkt point-str)
                                                                  'geom))))
             'admin-level))
     (let ((name-only nil))
       (block scan-nearest
         (dolist
             (b
               (query (:order-by
                       (:select 'building-poly.id
                                (:as
                                 (:st-distance-sphere 'geom
                                                      (:st-geomfromewkt point-str))
                                 'dist)
                                'name 'street 'housenumber
                                :from 'building-poly
                                :left-join 'building
                                :on (:= 'building-poly.id 'building.id)
                                :where (:st-dwithin 'geom (:st-geomfromewkt point-str) 0.001))
                       'dist)))
           (when (and (not name-only)
                      (or (eq (fourth b) :null) (eq (fifth b) :null))
             (setf name-only b)))
           (when (and (not (eq (fourth b) :null)) (not (eq (fifth b) :null)))
             (return-from scan-nearest (list b name-only))))
         (list nil name-only))))))

;; (time
;;  (dolist (c *coord*)
;;    (let ((cc (db:query-point (car c) (cadr c))))
;;      (when (or (first (second cc)) (second (second cc)))
;;        (format t "========~%~A~%" c)
;;        (format t "~A~%" cc)))))

(defun update-stringtable (st)
  (let ((idx (make-array (length st) :element-type '(unsigned-byte 64))))
    (loop for os across st
       for i from 0
       do (let* ((s (sb-ext:octets-to-string os))
                 (sb-id (query (:select 'id :from 'stringtable :where (:= 's s)) :single))
                 (new-id (unless sb-id
                           (let ((new-id
                                  (query (:select (:nextval "st_seq")) :single)))
                             (execute (:insert-into 'stringtable :set
                                                    'id new-id
                                                    's s))
                             new-id))))
            (setf (aref idx i) (or new-id sb-id))))
    idx))

(defun store-node (id lon lat tags)
  (let ((point-str (format nil "SRID=4326;POINT(~F ~F)" lon lat)))
    (with-transaction ()
      (execute (:insert-into 'node :set
                             'id id
                             'point (:st-geomfromewkt point-str)))
      (dolist (tag tags)
        (execute (:insert-into 'node-tags :set
                               'node-id id
                               'key-id (car tag)
                               'val-id (cdr tag)))))))

(defun store-way (id refs tags)
  (with-transaction ()
    (execute (:insert-into 'way :set 'id id))
    (dolist (ref refs)
      (execute (:insert-into 'way-refs :set
                             'way-id id
                             'node-id ref)))
    (dolist (tag tags)
      (execute (:insert-into 'way-tags :set
                             'way-id id
                             'key-id (car tag)
                             'val-id (cdr tag))))))

(defun store-relation (id members tags)
  (with-transaction ()
    (execute (:insert-into 'relation :set 'id id))
    (dolist (member members)
      (execute (:insert-into 'relation-members :set
                             'relation-id id
                             'member-id (car member)
                             'member-type (cdr member))))
    (dolist (tag tags)
      (execute (:insert-into 'relation-tags :set
                             'relation-id id
                             'key-id (car tag)
                             'val-id (cdr tag))))))
