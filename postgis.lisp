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
           :write-boundary
           :create-boundary-polys
           :write-building-rel
           :write-building-way
           :create-building-polys
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
    (when (< (length points-list) 2) (return-from make-way-linestring nil))
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

(defun create-boundary-polys ()
  (with-transaction ()
    (execute "select boundary.id as id, (st_dump(st_polygonize(geom))).geom as geom into boundary_poly from boundary left join relation_ways on (boundary.id = rel_id) left join way on (way_id = way.id) group by boundary.id")
    (execute "CREATE INDEX boundary_poly_geom on boundary_poly using gist(geom)")))

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

(defun create-building-polys ()
  (with-transaction ()
    (execute "select building.id as id, (st_dump(st_polygonize(geom))).geom as geom into building_poly from building left join relation_ways on (building.id = rel_id) left join way on (way_id = way.id)where is_rel = true group by building.id")
    (execute "insert into building_poly select building.id as id, (st_dump(st_polygonize(geom))).geom as geom from building left join way on (building.id = way.id) where is_rel = false group by building.id")
    (execute "CREATE INDEX building_poly_geom on building_poly using gist(geom)")))

;; select relation.id, astext(st_polygonize(geom)) from relation left join relation_ways on (relation.id = rel_id) left join way on (way_id = way.id) group by relation.id;
;; select relation.id, st_polygonize(geom) as geom into relation_poly from relation left join relation_ways on (relation.id = rel_id) left join way on (way_id = way.id) group by relation.id;

;; select relation.id as id, (st_dump(st_polygonize(geom))).geom as geom from relation left join relation_ways on (relation.id = rel_id) left join way on (way_id = way.id) group by relation.id;
;; select relation.id as id, (st_dump(st_polygonize(geom))).geom as geom into relation_poly from relation left join relation_ways on (relation.id = rel_id) left join way on (way_id = way.id) group by relation.id;
;; CREATE INDEX relation_poly_geom on relation_poly using gist(geom);
;; SELECT id, st_within(st_geomfromewkt('SRID=4326;POINT(35.209808 47.881355)'), geom) from relation_poly;
;; SELECT id from relation_poly where st_within(st_geomfromewkt('SRID=4326;POINT(35.209808 47.881355)'), geom);

;; SELECT id, st_distance_sphere(geom, st_geomfromewkt('SRID=4326;POINT(35.209808 47.881355)')) as dist from building_poly where st_dwithin(geom, st_geomfromewkt('SRID=4326;POINT(35.209808 47.881355)'), 0.1) order by dist limit 1;
;; SELECT building_poly.id, st_distance_sphere(geom, st_geomfromewkt('SRID=4326;POINT(35.209808 47.881355)')) as dist, name, street, housenumber from building_poly left join building on (building_poly.id = building.id) where st_dwithin(geom, st_geomfromewkt('SRID=4326;POINT(35.209808 47.881355)'), 0.1) order by dist;

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
