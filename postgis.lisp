(defpackage :osm-postgis
  (:nicknames :db)
  (:use :cl
        :postmodern
        :s-sql
        :cl-postgres
        :b-tree
        :in-mem-str)
  (:export :default-connect
           :write-way
           :check-rel-members
           :write-rel
           :query-point))

(in-package :osm-postgis)

(defvar *db-host* "localhost")
(defvar *db-name* "gis")
(defvar *db-user* "gis")
(defvar *db-password* "gis")

(defun default-connect ()
  (when *database* (disconnect-toplevel))
  (connect-toplevel *db-name* *db-user* *db-password* *db-host*))

(defun make-way-linestring (way nodes-btree)
  (let ((points-list
         (loop for nid across (way-refs way)
            collect (let ((node (bsearch nodes-btree nid)))
                      (unless node (return-from make-way-linestring nil))
                      ;; FIXME! hardcoded offsets and granularity 
                      (format nil "~F ~F"
                              (* .000000001 100 (node-lon node))
                              (* .000000001 100 (node-lat node)))))))
    (format nil "SRID=4326;LINESTRING(~A~{,~A~})" (car points-list) (cdr points-list))))

(defun write-way (way nodes-btree)
  (let ((ls (make-way-linestring way nodes-btree)))
    (when ls
      (execute
       (format nil "insert into way values (~D, st_geomfromewkt(~A))" (way-id way) (sql-escape ls)))
      t)))


(defun check-rel-members (rel)
  "Check if all necessary relation memebers (only check for ways now) exist in database"
  (let ((found nil))
    (loop for mem across (relation-members rel)
       do
         (when (= (rel-member-type mem) osmpbf:+relation-member-type-way+)
           (if (query (:select 'id :from 'way :where (:= 'id (rel-member-id mem))) :single)
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

(defun write-rel (rel)
  (with-transaction ()
    (execute (:insert-into 'relation
                           :set
                           'id (relation-id rel)
                           'name (tag-value (find-tag (relation-tags rel) "name") :NULL)
                           'admin-level (let ((val (tag-value (find-tag (relation-tags rel) "admin_level") "-1")))
                                          (if (integerp (read-from-string val)) val -1))))
    (loop for mem across (relation-members rel)
         do
         (when (= (rel-member-type mem) osmpbf:+relation-member-type-way+)
           (execute (:insert-into 'relation_ways
                                  :set
                                  'rel-id (relation-id rel)
                                  'way-id (rel-member-id mem)))))))

;; select relation.id, astext(st_polygonize(geom)) from relation left join relation_ways on (relation.id = rel_id) left join way on (way_id = way.id) group by relation.id;
;; select relation.id, st_polygonize(geom) as geom into relation_poly from relation left join relation_ways on (relation.id = rel_id) left join way on (way_id = way.id) group by relation.id;

;; select relation.id as id, (st_dump(st_polygonize(geom))).geom as geom from relation left join relation_ways on (relation.id = rel_id) left join way on (way_id = way.id) group by relation.id;
;; select relation.id as id, (st_dump(st_polygonize(geom))).geom as geom into relation_poly from relation left join relation_ways on (relation.id = rel_id) left join way on (way_id = way.id) group by relation.id;
;; CREATE INDEX relation_poly_geom on relation_poly using gist(geom);
;; SELECT id, st_within(st_geomfromewkt('SRID=4326;POINT(35.209808 47.881355)'), geom) from relation_poly;
;; SELECT id from relation_poly where st_within(st_geomfromewkt('SRID=4326;POINT(35.209808 47.881355)'), geom);

(defun query-point (lon lat)
  (query (:order-by
          (:select '* :from 'relation
                   :where (:in 'id (:select 'id
                                            :from 'relation-poly
                                            :where (:st-within (:st-geomfromewkt (format nil "SRID=4326;POINT(~F ~F)" lon lat))
                                                               'geom))))
          'admin-level)))
