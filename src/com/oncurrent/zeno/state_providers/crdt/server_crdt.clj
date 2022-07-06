(ns com.oncurrent.zeno.state-providers.crdt.server-crdt
  (:require
   [com.oncurrent.zeno.state-providers.crdt.common :as c]
   [com.oncurrent.zeno.utils :as u]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.fingerprint :as lfp]
   [taoensso.timbre :as log])
  (:import
   (java.nio ByteBuffer)
   (java.util Arrays)
   (jdk.incubator.foreign MemorySegment
                          ValueLayout)
   (jdk.incubator.vector Vector)))

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;; Server CRDT Op Byte Array Layout ;;;;;;;;;;;;;;;;;;;;
;;
;; 256 Bytes long (4 cache lines)
;;
;; 0                   1                   2                   3
;; 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
;; +-+-+-+-+-------+----------------+------------------------------+
;; |V|O|V|P|       |                |                              |
;; |E|T|T|L| Path  | System time    | Add                          |
;; |R|Y|Y|E| Header| Milliseconds   | ID                           |
;; | |P|P|N| (4)   | (8)            | (16)                         |
;; +-+-+-+-+-------+----------------+------------------------------+
;; |                                                               |
;; | Path
;; |                                                               |
;; | (192)
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
;; +---------------------------------------------------------------+
;; |                                                               |
;; | Value                                                         |
;; |                                                               |
;; | (32)                                                          |
;; +---------------------------------------------------------------+
;;
;;  VER = Version
;;  OTYP = Op Type
;;  VTYP = Value Type
;;  PLEN = Path Length
;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def op-byte-array-len 256)

;; WARNING: Only add to the bottom of this list; do not insert in the
;; middle. Doing so will change the meaning of stored data.
(def op-types [:add-array-edge
               :add-container
               :add-value
               :delete-array-edge
               :delete-container
               :delete-value])

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
        kw->i (reduce (fn [acc i]
                        (assoc acc (i->kw i) i))
                      {}
                      (range (count i->kw)))]
    (u/sym-map kw->i i->kw)))

(defn insert-ba [ba index item]
  (let [len (count ba)
        new-ba ^bytes (byte-array (inc len))
        old-mem-seg ^MemorySegment (MemorySegment/ofArray ^bytes ba)
        new-mem-seg ^MemorySegment (MemorySegment/ofArray ^bytes new-ba)]
    (MemorySegment/copy old-mem-seg 0
                        new-mem-seg 0
                        (long index))
    (.set new-mem-seg ValueLayout/JAVA_BYTE (long index) ^byte item)
    (MemorySegment/copy old-mem-seg (long index)
                        new-mem-seg (long (inc index))
                        (long (- len index)))
    new-ba))

(defn write-path! [path ^MemorySegment mem-seg]
  #_(let [path-len (count path)
          last-i (dec path-len)]
      (when (pos? path-len)
        (loop [i 0]
          (let [el (nth path i)
                s (cond
                    (keyword? el)
                    (str keyword-marker el)

                    (string? el)
                    (str string-marker el)

                    :else
                    (throw (ex-info
                            (str "Bad path element `" (or el "nil") "`.")
                            (u/sym-map path el))))]
            (.set mem-seg
                  ValueLayout/JAVA_LONG
                  (long (* i path-element-len))
                  )
            (when (< i last-i)
              (recur (inc i)))))
        (.set mem-seg xs!
              ValueLayout/JAVA_BYTE
              (long path-len-offset)
              (byte path-len)))))

(defn ->op-byte-array [{:keys [path schema kw->i]}]
  #_(let [ba (byte-array op-byte-array-len)
          mem-seg ^MemorySegment (MemorySegment/ofArray ^bytes ba)
          path-v (vec path)]
      (reduce
       (fn [offset path-i]
         (let [el (nth path-v path-i)
               [l v] (cond
                       (keyword? el)
                       (let [kw-i (kw->i el)
                             num-bytes (cond <)
                             l (unchecked-byte
                                (bit-or (unchecked-byte 2r10000000)
                                        ))])
                       (str keyword-marker el)

                       (string? el)
                       (str string-marker el)

                       :else
                       (throw (ex-info
                               (str "Bad path element `" (or el "nil") "`.")
                               (u/sym-map path el))))]
           (if)))
       32
       (range (count path-v)))
      ba))
