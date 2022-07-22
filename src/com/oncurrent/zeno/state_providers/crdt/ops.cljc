(ns com.oncurrent.zeno.state-providers.crdt.ops
  (:require
   [com.oncurrent.zeno.state-providers.crdt.common :as c]
   [com.oncurrent.zeno.state-providers.crdt.shared :as shared]
   [com.oncurrent.zeno.utils :as u]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.stockroom :as sr]
   [taoensso.timbre :as log])
  #?(:clj
     (:import
      (jdk.incubator.foreign MemorySegment
                             ValueLayout)
      (jdk.incubator.vector Vector))))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;; CRDT Op Byte Array Layout ;;;;;;;;;;;;;;;;;;;;
;;
;; 256 Bytes long (4 cache lines)
;;
;;  0                   1                   2                   3
;;  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
;; +-+-+-+-+-------+---------------+-------------------------------+
;; |V|O|V|P|       |               |                               |
;; |E|T|T|L| Path  | System time   | Add                           |
;; |R|Y|Y|E| Header| Milliseconds  | ID                            |
;; | |P|P|N| (4)   | (8)           | (16)                          |
;; +-+-+-+-+-------+---------------+-------------------------------+
;; |                                                               |
;; | Path
;; |                                                               |
;; | (176)
;; +- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -+
;; |                                                               |
;;   Path
;; | (cont'd)                                                      |
;;
;; +- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -+
;; |                                                               |
;;   Path
;; | (cont'd)                                                      |
;;
;; +- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -+
;; |                                                               |
;;   Path
;; | (cont'd)                                                      |
;;
;; +- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -+
;; |                                                               |
;;   Path
;; | (cont'd)                                                      |
;;
;; +- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -+
;; |                                                               |
;;   Path                                                          |
;; | (cont'd)                                                      |
;;                                                                 |
;; +-------------------------------+-------------------------------+
;; |                               |                               |
;; | Schema MD5                    | Value                         |
;; | Fingerprint                   |                               |
;; | (16)                          | (16)                          |
;; +-------------------------------+-------------------------------+
;;
;;  VER = Version (This is version 1)
;;  OTYP = Op Type
;;  VTYP = Value Type
;;  PLEN = Path Length (signed)
;;
;;  In the path header, there is one bit per path element. A 0 bit indicates
;;  a keyword. A 1 bit indicates a string. There are up to 32 path elements.
;;
;;  Keyword path elements are encoded using the signed 2-byte representation
;;  of the keyword's index as given by the kw->i map.
;;
;;  String path elements are encoded using the 16-byte MD5 hash of the string.
;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def add-id-num-bytes 16)
(def add-id-offset 16)
(def kw-num-bytes 2)
(def hash-num-bytes 16)
(def max-num-kws (dec (bit-shift-left 1 (dec (* 8 kw-num-bytes)))))
(def max-path-len 32)
(def op-byte-array-len 256)
(def op-byte-array-version 1)
(def op-byte-array-version-offset 0)
(def op-type-offset 1)
(def path-header-offset 4)
(def path-len-offset 3)
(def path-start-offset 32)
(def schema-fp-offset 224)
(def schema-fp-num-bytes 16)
(def sys-time-ms-offset 8)
(def value-num-bytes 16)
(def value-offset 240)
(def value-type-offset 2)

(def __fp->kw-fns-cache__ (sr/stockroom 100))

(defmulti get-kws-in-schema c/schema->dispatch-type)

(defmethod get-kws-in-schema :array
  [schema]
  (get-kws-in-schema (l/child-schema schema)))

(defmethod get-kws-in-schema :map
  [schema]
  (get-kws-in-schema (l/child-schema schema)))

(defmethod get-kws-in-schema :record
  [schema]
  (let [{:keys [fields]} (l/edn schema)]
    (mapcat (fn [{field :name}]
              (let [child-schema (l/child-schema schema field)]
                (cons field (get-kws-in-schema child-schema))))
            fields)))

(defmethod get-kws-in-schema :union
  [schema]
  (mapcat get-kws-in-schema (l/member-schemas schema)))

(defmethod get-kws-in-schema :single-value
  [schema]
  nil)

(defn schema->kw-fns [schema]
  (let [i->kw (-> (get-kws-in-schema schema)
                  (set)
                  (sort)
                  (vec))
        _ (when (> (count i->kw) max-num-kws)
            (throw (ex-info (str "Schema contains more than " max-num-kws
                                 " path keywords (fields).")
                            {:num-kws (count i->kw)
                             :max-num-kws max-num-kws})))
        kw->i (reduce (fn [acc i]
                        (assoc acc (i->kw i) i))
                      {}
                      (range (count i->kw)))]
    (u/sym-map kw->i i->kw)))

(defn insert-ba-clj [ba index ba-to-insert]
  (let [len (count ba)
        new-ba ^bytes (byte-array (inc len))
        old-mem-seg ^MemorySegment (MemorySegment/ofArray ^bytes ba)
        new-mem-seg ^MemorySegment (MemorySegment/ofArray ^bytes new-ba)
        to-insert-mem-seg ^MemorySegment (MemorySegment/ofArray
                                          ^bytes ba-to-insert)]
    (MemorySegment/copy old-mem-seg 0
                        new-mem-seg 0
                        (long index))
    (MemorySegment/copy to-insert-mem-seg 0
                        new-mem-seg (long index)
                        (long (count ba-to-insert)))
    (MemorySegment/copy old-mem-seg (long index)
                        new-mem-seg (long (inc index))
                        (long (- len index)))
    new-ba))

(defn insert-ba [ba index ba-to-insert]
  #?(:clj
     (insert-ba-clj ba index ba-to-insert)))

(defn value->ba [{:keys [op-path schema value]}]
  (let [value-schema (get-value-schema schema op-path)
        value-schema-type (l/schema-type value-schema)
        value-ba (if-not (c/bytes-types value-schema-type)
                   (l/serialize value-schema value)
                   (let []))]
    (u/sym-map value-ba value-union-branch)))

#?(:clj
   (defn write-op-clj! [{:keys [kw->i offset op schema] :as arg}]
     (let [{:keys [add-id op-type op-path
                   value value-type value-union-branch]} op
           mem-seg ^MemorySegment (:mem-seg arg)
           path-v (vec op-path)
           path-len (count path-v)
           add-id-mem-seg ^MemorySegment (MemorySegment/ofArray ^bytes add-id)
           _ (when (> path-len max-path-len)
               (throw (ex-info (str "Op path length can't exceed " max-path-len
                                    ". Got " path-len ".")
                               (u/sym-map op-path path-len))))
           _ (.set mem-seg
                   ValueLayout/JAVA_BYTE
                   (long op-byte-array-version-offset)
                   (byte op-byte-array-version))
           _ (.set mem-seg
                   ValueLayout/JAVA_BYTE
                   (long op-type-offset)
                   (byte (aget ^bytes (l/serialize shared/op-type-schema
                                                   op-type)
                               0)))
           _ (when value-union-branch
               (.set mem-seg
                     ValueLayout/JAVA_BYTE
                     (long value-union-branch-offset)
                     (byte value-union-branch)))
           _ (.set mem-seg
                   ValueLayout/JAVA_BYTE
                   (long path-len-offset)
                   (byte path-len))
           _ (.set mem-seg
                   ValueLayout/JAVA_LONG
                   (long sys-time-ms-offset)
                   (long (or (:sys-time-ms op)
                             (:sys-time-ms arg))))
           _ (MemorySegment/copy add-id-mem-seg 0
                                 mem-seg add-id-offset
                                 (count add-id))
           info (reduce
                 (fn [{:keys [offset path-header] :as acc} path-i]
                   (let [el (nth path-v path-i)]
                     (cond
                       (keyword? el)
                       (let [kw-i (kw->i el)]
                         (.set mem-seg
                               ValueLayout/JAVA_SHORT
                               (long offset)
                               (short kw-i))
                         (update acc :offset + kw-num-bytes))


                       (string? el)
                       (let [string-ba (ba/utf8->byte-array el)
                             hash-ba (ba/md5 string-ba)
                             hash-mem-seg ^MemorySegment (MemorySegment/ofArray
                                                          ^bytes hash-ba)]
                         ;; TODO: Ensure hash-ba is in the string table
                         (MemorySegment/copy hash-mem-seg 0
                                             mem-seg offset
                                             hash-num-bytes)
                         (-> acc
                             (update :offset + hash-num-bytes)
                             (update :path-header bit-set path-i)))

                       :else
                       (throw (ex-info
                               (str "Bad op path element `" (or el "nil") "`.")
                               (u/sym-map op-path el))))))
                 {:offset path-start-offset
                  :path-header (int 0)}
                 (range path-len))
           _ (.set mem-seg
                   ValueLayout/JAVA_INT
                   (long path-header-offset)
                   (unchecked-int (:path-header info)))
           fp-ba-mem-seg ^MemorySegment (MemorySegment/ofArray
                                         ^bytes (l/fingerprint128 schema))
           _ (MemorySegment/copy fp-ba-mem-seg 0
                                 mem-seg schema-fp-offset
                                 schema-fp-num-bytes)
           value-ba-mem-seg ^MemorySegment (MemorySegment/ofArray
                                            ^bytes value-ba)]
       (MemorySegment/copy value-ba-mem-seg 0
                           mem-seg value-offset
                           (count value-ba)))))

(defn write-op! [arg]
  #?(:clj
     (write-op-clj! arg)))

(defn ->ops-byte-array [{:keys [ops schema sys-time-ms] :as arg}]
  (let [kw-fns (or (sr/get __fp->kw-fns-cache__ (l/fingerprint128 schema))
                   (let [fns (schema->kw-fns schema)]
                     (sr/put! __fp->kw-fns-cache__
                              (l/fingerprint128 schema)
                              fns)
                     fns))
        ba (byte-array (* op-byte-array-len (count ops)))
        mem-seg #?(:clj (MemorySegment/ofArray ^bytes ba)
                   :cljs nil)]
    #?(:clj (.fill ^MemorySegment mem-seg (byte 0)))
    (reduce (fn [offset op]
              (write-op! (assoc arg
                                :ba ba
                                :i->kw (:i->kw kw-fns)
                                :kw->i (:kw->i kw-fns)
                                :mem-seg mem-seg
                                :offset offset
                                :op op
                                :sys-time-ms (or sys-time-ms
                                                 (u/current-time-ms))))
              (+ offset op-byte-array-len))
            0
            ops)
    ba))
