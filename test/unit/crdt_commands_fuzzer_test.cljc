(ns unit.crdt-commands-fuzzer-test
  (:require
   [deercreeklabs.lancaster :as l]
   [clojure.data :as data]
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.test :as t :refer [deftest is]]
   [com.oncurrent.zeno.state-providers.crdt.commands :as commands]
   [com.oncurrent.zeno.state-providers.crdt.common :as crdt]
   [com.oncurrent.zeno.state-providers.crdt.get :as get]
   [com.oncurrent.zeno.utils :as u]
   [unit.crdt-commands-test :as ct]
   #?(:clj [kaocha.repl])
   [taoensso.timbre :as log])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(comment (kaocha.repl/run *ns* {:capture-output? false}))

(defn prefixed-uuid [prefix]
  (str prefix (u/compact-random-uuid)))

(defn prefixed-uuids [n prefix]
  (map #(str prefix %) (u/compact-random-uuids n)))

(defn gen-delete-cmd [i]
  {:zeno/op :zeno/remove
   :zeno/path [i]})

(defn gen-insert-before-cmd
  ([i]
   (gen-insert-before-cmd i ""))
  ([i prefix]
   {:zeno/op :zeno/insert-before
    :zeno/path [i]
    :zeno/arg (prefixed-uuid prefix)}))

(defn gen-insert-after-cmd
  ([i]
   (gen-insert-after-cmd i ""))
  ([i prefix]
   {:zeno/op :zeno/insert-after
    :zeno/path [i]
    :zeno/arg (prefixed-uuid prefix)}))

(defn gen-insert-range-before-cmd
  ([i]
   (gen-insert-range-before-cmd i ""))
  ([i prefix]
   {:zeno/op :zeno/insert-range-before
    :zeno/path [i]
    :zeno/arg (prefixed-uuids 5 prefix)}))

(defn gen-insert-range-after-cmd
  ([i]
   (gen-insert-range-after-cmd i ""))
  ([i prefix]
   {:zeno/op :zeno/insert-range-after
    :zeno/path [i]
    :zeno/arg (prefixed-uuids 5 prefix)}))

(defn rand-cmds-from-coll-of-strs
  ([coll]
   (rand-cmds-from-coll-of-strs coll ""))
  ([coll prefix]
   (keep-indexed (fn [i v]
                   (case (int (rand-int 6))
                     0 (gen-delete-cmd i)
                     2 (gen-insert-before-cmd i prefix)
                     1 (gen-insert-after-cmd i prefix)
                     4 (gen-insert-range-before-cmd i prefix)
                     3 (gen-insert-range-after-cmd i prefix)
                     5 nil))
                 coll)))

(defn get-deleted-values-as-set [data cmds]
  (->> cmds
       (filter (fn [{:zeno/keys [op]}]
                 (= :zeno/remove op)))
       (map (comp #(nth data %) first :zeno/path))
       (into #{})))

(defn filter-out-values [values-set data]
  (filter #(not (contains? values-set %)) data))

(defn keep-by-prefix [prefix data]
  (filter #(str/starts-with? % prefix) data))

(defn partition-by-prefix [prefix delimiter data]
  (partition-by #(= prefix (->> (re-pattern delimiter) (str/split %) (first)))
                data))

(defn partition-by-prefixes [delimeter data]
  (partition-by #(->> (re-pattern delimeter) (str/split %) (first)) data))

(defn drop-partitions-by-prefix [prefix data]
  (filter #(not (str/starts-with? (first %) prefix)) data))

;; TODO; Finish getting this test to pass with the hardcoded case and then
;; with the full randomness.
#_(comment (kaocha.repl/run #'test-random-three-way-array-merge
                          {:capture-output? false :color? false}))
#_(deftest test-random-three-way-array-merge []
  (let [;; First, we set up an initial CRDT and ensure it's correct.
        schema (l/array-schema l/string-schema)
        ; data (prefixed-uuids 3 "zeroth-")
        prefix0 "0"  #_"zeroth"
        prefix1 "1"  #_"first"
        prefix2 "2"  #_"second"
        prefix3 "3"  #_"third"
        delimiter "-"
        data [(str prefix0 delimiter "a")
              #_"zeroth-1xxvagjenwgvzbqkh1rmvkxcwd"
              #_"zeroth-9akvybdp9rj388bhjq65qx0451"
              #_"zeroth-669nha6hw2gzmaab4xw4jxw4wd"]
        *id (atom 0)
        make-id (fn []
                  (let [id @*id]
                    (swap! *id inc)
                    (str id)))
        cmds [{:zeno/arg data
                    :zeno/op :zeno/set
                    :zeno/path []}]
        {:keys [crdt crdt-ops]} (ct/process-cmds cmds schema {:make-id make-id})
        acrdt (ct/ops->crdt crdt-ops schema)
        _ (is (= data
                 (ct/->value crdt [] schema)
                 (ct/->value acrdt [] schema)))
        ;; Next, we generate two sets of random commands concurrently (they
        ;; have no knowledge of each other).
        ; cmds1 (rand-cmds-from-coll-of-strs data "first-")
        ; cmds2 (rand-cmds-from-coll-of-strs data "second-")
        ; cmds3 (rand-cmds-from-coll-of-strs data "third-")
        cmds1 [#:zeno{:op :zeno/insert-range-before
                      :path [0]
                      :arg [(str prefix1 delimiter "a")
                            #_"first-82bzy3z8w0kvyaf6d9307kzy48"
                            #_"first-78kkvbt568jpe9pdwyvpm9yy5q"
                            #_"first-114gxat3r6jrx8vw29bdeaawvb"
                            #_"first-36hj8ehvswjnz8awxyx20pvvdb"
                            #_"first-860sa340xrkjva6zq3fx95zmyc"]}
               #:zeno{:op :zeno/insert-after
                      :path [1]
                      :arg (str prefix1 delimiter "b") #_"first-01nqybma9eg6ebag41e8m6y4jz"}
               #:zeno{:op :zeno/insert-range-after
                      :path [2]
                      :arg [(str prefix1 delimiter "c")
                            #_"first-bm9sfe9r16hc5b8z3mkgwzzyh6"
                            #_"first-avgcjpd82ej5dbt8afcbnjxp6t"
                            #_"first-b41xjtz8a6ggmb8ehvbxtwcxmm"
                            #_"first-0zjazgzcr4jp496cg6raw80jm1"
                            #_"first-96hnx0h58ygc586c66ykj6pj4e"]}]
        cmds2 [#:zeno{:op :zeno/insert-range-after
                      :path [0]
                      :arg [(str prefix2 delimiter "a")
                            #_"second-en2bmdgprmhtgbzm0kzakg7s0d"
                            #_"second-25v9bj6v0tg17bcy1ehm6jcstw"
                            #_"second-922bz80e4wght95np90p0rtsgr"
                            #_"second-fj34agdrvcjxxbd9ksjf836es0"
                            #_"second-eh102et83ek169cmyx8wasys3h"]}
               #:zeno{:op :zeno/insert-after
                      :path [1]
                      :arg (str prefix2 delimiter "b") #_"second-62r7y3t7pgjrv8ctrc7rm9382j"}
               #:zeno{:op :zeno/insert-range-before
                      :path [2]
                      :arg [(str prefix2 delimiter "c")
                            #_"second-975q32239whqr8egpsx60fevv8"
                            #_"second-3c8jhwjr76jg2b2zsrjhmmeaf8"
                            #_"second-dk2txhjp54jwyb63hk7k6hy4jq"
                            #_"second-f6rvmtprp2jj2a9212nzdgsd3f"
                            #_"second-4ejrxged0rh2x8t8hq2j5zn4bh"]}]
        cmds3 [#:zeno{:op :zeno/remove
                      :path [0]}
               #:zeno{:op :zeno/insert-range-after
                      :path [0] #_[1]
                      :arg [(str prefix3 delimiter "a")
                            #_"third-6eezfx1xzrk48by16pqz7c0peb"
                            #_"third-eacbe6zje8gkg96hage370s2rp"
                            #_"third-atmg391fxtgyhb01bzt5sbpqyy"
                            #_"third-cgjj58zxcmk1v8k0hn9p2hjdx8"
                            #_"third-2jz1pdba1rgvrbvdv34nx5fcha"]}]
        ;; Process each set of commands and extract the value from the
        ;; resulting CRDTs as well as construct CRDTs from the returned ops.
        other {:crdt crdt :make-id make-id}
        {crdt1 :crdt crdt-ops1 :crdt-ops} (ct/process-cmds cmds1 schema other)
        {crdt2 :crdt crdt-ops2 :crdt-ops} (ct/process-cmds cmds2 schema other)
        {crdt3 :crdt crdt-ops3 :crdt-ops} (ct/process-cmds cmds3 schema other)
        acrdt1 (ct/ops->crdt (set/union crdt-ops crdt-ops1) schema)
        acrdt2 (ct/ops->crdt (set/union crdt-ops crdt-ops2) schema)
        acrdt3 (ct/ops->crdt (set/union crdt-ops crdt-ops3) schema)
        data1 (ct/->value crdt1 [] schema)
        data2 (ct/->value crdt2 [] schema)
        data3 (ct/->value crdt3 [] schema)
        ;; Some consistency checks for each CRDT individually.
        _ (is (= data1
                 (ct/->value acrdt1 [] schema)))
        _ (is (= data2
                 (ct/->value acrdt2 [] schema)))
        _ (is (= data3
                 (ct/->value acrdt3 [] schema)))
        ;; Create the combined CRDT both ways, extract value, and assert
        ;; equality.
        new-ops (set/union crdt-ops1 crdt-ops2 crdt-ops3)
        ops-all (set/union crdt-ops new-ops)
        acrdt-all (ct/ops->crdt ops-all schema)
        mcrdt (ct/ops->crdt new-ops schema {:crdt crdt})
        data-all (ct/->value acrdt-all [] schema)
        _ (is (= data-all (ct/->value mcrdt [] schema)))
        _ (log/info data)
        _ (log/info data1)
        _ (log/info data2)
        _ (log/info data3)
        _ (log/info data-all)
        ;; Because the values are UUIDs we can use sets instead of vectors
        ;; for the next two parts
        set1 (into #{} data1)
        set2 (into #{} data2)
        set3 (into #{} data3)
        set-all (into #{} data-all)
        ;; Test that deleted things are not present post merge.
        deleted1 (get-deleted-values-as-set data cmds1)
        deleted2 (get-deleted-values-as-set data cmds2)
        deleted3 (get-deleted-values-as-set data cmds3)
        deleted-all (set/union deleted1 deleted2 deleted3)
        _ (is (empty? (set/intersection set-all deleted-all)))
        ;; Test that all not-deleted things are present post merge.
        _ (is (= (set/difference set1 deleted-all) (set/intersection set-all set1)))
        _ (is (= (set/difference set2 deleted-all) (set/intersection set-all set2)))
        _ (is (= (set/difference set3 deleted-all) (set/intersection set-all set3)))
        ;; Test that all zeroth things are in the correct order, cmds can't
        ;; rearrage without deleting and readding but we aren't doing that
        ;; since each new insertion is a new UUID.
        data-post-merge-expected (filter-out-values
                                  (set/union deleted1 deleted2 deleted3) data)
        data-post-merge-actual (keep-by-prefix prefix0 data-all)
        _ (is (= data-post-merge-expected data-post-merge-actual))
        ;; Test that at any given position, i.e. before or after an element
        ;; of the original array, there is not interleaving and that
        ;; whichever comes first at in one position comes first in all
        ;; positions. We start by creating partitions of all prefixes between
        ;; all original elements.
        partitioned (->> data-all
                         (partition-by-prefix prefix0 delimiter)
                         (drop-partitions-by-prefix prefix0)
                         (map #(partition-by-prefixes delimiter %)))
        *came-first (atom nil)
        *came-second (atom nil)
        *came-last (atom nil)
        ]
    (doseq [p partitioned]
      (case (count p)
        0 nil ; Not a problem, perhaps no commands were generated.
        1 nil ; Not a problem, testing would only be testing partition-by.
        2 (do ; I can only test this if I've seen the 3 case below so I know what the order should be.
              (when (and @*came-first @*came-second @*came-last)
                (is
                 (or
                  (and (every? #(str/starts-with? % @*came-first) (first p))
                       (every? #(str/starts-with? % @*came-second) (second p)))
                  (and (every? #(str/starts-with? % @*came-first) (first p))
                       (every? #(str/starts-with? % @*came-last) (last p)))
                  (and (every? #(str/starts-with? % @*came-second) (second p))
                       (every? #(str/starts-with? % @*came-last) (last p)))))))
        3 (do
           (when-not @*came-first
             (reset! *came-first (-> p ffirst (str/split #"-") first)))
           (when-not @*came-second
             (reset! *came-second (-> p second first (str/split #"-") first)))
           (when-not @*came-last
             (reset! *came-last (-> p last first (str/split #"-") first)))
           (is (and (every? #(str/starts-with? % @*came-first) (first p))
                    (every? #(str/starts-with? % @*came-second) (second p))
                    (every? #(str/starts-with? % @*came-last) (last p)))))
        (is (= "commands interleaved" "but should not have"))))))
