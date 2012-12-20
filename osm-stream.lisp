(in-package :osm-reader)

(defun unpack-kv-stringtable (kv-arr st-idx)
  (do ((res nil)
       (cur-res nil)
       (kv-idx 0)
       (kv-size (length kv-arr)))
      ((>= kv-idx kv-size) (progn
                             (when cur-res
                               (push cur-res res))
                             (nreverse res)))
    (let ((key (aref kv-arr kv-idx)))
      (incf kv-idx)
      (if (zerop key)
          (progn
            (push cur-res res)
            (setf cur-res nil))
          (let ((val (aref kv-arr kv-idx)))
            (incf kv-idx)
            (push (cons (aref st-idx key)
                        (aref st-idx val)) cur-res))))))

(defun stream-dense (dn st-idx gran lat-offs lon-offs)
  (let ((prev-id nil)
        (prev-lat nil)
        (prev-lon nil)
        (all-tags (unpack-kv-stringtable (osmpbf:keys-vals dn) st-idx)))
    (loop for id across (osmpbf:id dn)
       for lon across (osmpbf:lon dn)
       for lat across (osmpbf:lat dn)
       for tags in all-tags
       do (progn
            (when prev-id
              (setf id (+ id prev-id)
                    lat (+ lat prev-lat)
                    lon (+ lon prev-lon)))
            (db:store-node id
                           (* .000000001 (+ lon-offs (* gran lon)))
                           (* .000000001 (+ lat-offs (* gran lat)))
                           tags)
            (setf prev-id id
                  prev-lat lat
                  prev-lon lon)))))

(defun stream-osm-data (data)
  "stream osm data to postgis database"
  (let ((pblock (make-instance 'osmpbf:primitive-block)))
    (pb:merge-from-array pblock data 0 (length data))
    (let ((st-idx (db:update-stringtable (osmpbf:s (osmpbf:stringtable pblock)))))
      (let ((pgroup-arr (osmpbf:primitivegroup pblock)))
        (loop for pgroup across pgroup-arr
             do (progn
                  (when (osmpbf:has-dense pgroup)
                    (stream-dense (osmpbf:dense pgroup) st-idx
                                  (osmpbf:granularity pblock)
                                  (osmpbf:lat-offset pblock)
                                  (osmpbf:lon-offset pblock)))))))))
