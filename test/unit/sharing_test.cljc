(ns unit.sharing-test
  (:require
   [clojure.test :as t :refer [deftest is]]
   [deercreeklabs.lancaster :as l]
   [oncurrent.zeno.crdt :as crdt]
   [oncurrent.zeno.sharing :as sharing]
   [oncurrent.zeno.commands :as commands]
   [oncurrent.zeno.schemas :as schemas]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))


(deftest test-xf-set-cmd-dispatch
  (is (= :set-root (sharing/xf-set-cmd-dispatch {:path []})))
  (is (= :set-sg (sharing/xf-set-cmd-dispatch {:path ["id"]})))
  (is (= :set-sg-members (sharing/xf-set-cmd-dispatch
                          {:path ["id" :zeno/members]})))
  (is (= :set-sg-paths (sharing/xf-set-cmd-dispatch
                        {:path ["id" :zeno/paths]})))
  (is (= :set-sg-member-info (sharing/xf-set-cmd-dispatch
                              {:path ["id" :zeno/members "mid"]})))
  (is (= :set-sg-path (sharing/xf-set-cmd-dispatch
                       {:path ["id" :zeno/paths [:path1]]})))
  (is (= :set-sg-member-permissions
         (sharing/xf-set-cmd-dispatch
          {:path ["id" :zeno/members "mid" :zeno/permissions]})))
  (is (= :set-sg-member-permission
         (sharing/xf-set-cmd-dispatch
          {:path ["id" :zeno/members "mid" :zeno/permissions :zeno/read-data]})))
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"Sharing Group id must be a string"
       (sharing/xf-set-cmd-dispatch {:path [:not-a-string]})))
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"Unknown field"
       (sharing/xf-set-cmd-dispatch {:path ["id" :zeno/xxx]})))
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"Member id must be a string"
       (sharing/xf-set-cmd-dispatch {:path ["id" :zeno/members 1]})))
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"Path must be a sequence"
       (sharing/xf-set-cmd-dispatch {:path ["id" :zeno/paths :not-a-path]})))
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"Unknown field `:zeno/perms`"
       (sharing/xf-set-cmd-dispatch
        {:path ["id" :zeno/members "mid" :zeno/perms]})))
  (is (thrown-with-msg?
       #?(:clj ExceptionInfo :cljs js/Error)
       #"Unknown Sharing Group member permission"
       (sharing/xf-set-cmd-dispatch
        {:path ["id" :zeno/members "mid" :zeno/permissions :not-a-perm]}))))

(deftest test-write-permission-no-sharing
  (is (= true (sharing/can-write? {:sharing-store nil
                                   :actor-id "M1"
                                   :path [:zeno/crdt]}))))

(deftest test-read-permission-no-sharing
  (is (= true (sharing/can-read? {:sharing-store nil
                                  :actor-id "M1"
                                  :path [:zeno/crdt]}))))

(deftest test-sharing-set-root
  (let [*next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        sg1-id (make-id)
        sg2-id (make-id)
        member1-id (make-id)
        member2-id (make-id)
        member3-id (make-id)
        cmds [{:arg {sg1-id {:zeno/members {member1-id {:zeno/permissions
                                                        sharing/all-permissions}
                                            member2-id {:zeno/permissions
                                                        #{:zeno/read-data}}}
                             :zeno/paths #{[:a :b]}}
                     sg2-id {:zeno/members {member3-id {:zeno/permissions
                                                        sharing/all-permissions}}
                             :zeno/paths #{[:a]}}}
               :op :set
               :path [:zeno/sharing]}]
        ret (commands/process-cmds (u/sym-map cmds make-id))
        {:keys [sharing-store]} ret]
    (is (= true (sharing/can-write? {:sharing-store sharing-store
                                     :actor-id member1-id
                                     :path [:a :b]})))
    (is (= true (sharing/can-read? {:sharing-store sharing-store
                                    :actor-id member1-id
                                    :path [:a :b]})))
    (is (= false (sharing/can-write? {:sharing-store sharing-store
                                      :actor-id member2-id
                                      :path [:a :b]})))
    (is (= true (sharing/can-read? {:sharing-store sharing-store
                                    :actor-id member2-id
                                    :path [:a :b]})))
    (is (= false (sharing/can-write? {:sharing-store sharing-store
                                      :actor-id "somebody else"
                                      :path [:a :b]})))
    (is (= false (sharing/can-read? {:sharing-store sharing-store
                                     :actor-id "somebody else"
                                     :path [:a :b]})))
    (is (= false (sharing/can-read? {:sharing-store sharing-store
                                     :actor-id member3-id
                                     :path [:a :b]})))
    (is (= true (sharing/can-read? {:sharing-store sharing-store
                                    :actor-id member3-id
                                    :path [:a :c]})))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Sharing Group id must be a string"
         (commands/process-cmds
          {:cmds [{:arg {7 {}} ; 7 is not a string
                   :op :set
                   :path [:zeno/sharing]}]})))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Paths must be a set"
         (commands/process-cmds
          {:cmds [{:arg {sg1-id {:zeno/paths 7}}
                   :op :set
                   :path [:zeno/sharing]}]})))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Value of the `:zeno/members` key must be associative"
         (commands/process-cmds
          {:cmds [{:arg {sg1-id {:zeno/members 7}}
                   :op :set
                   :path [:zeno/sharing]}]})))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Member id must be a string"
         (commands/process-cmds
          {:cmds [{:arg {sg1-id {:zeno/members {7 {}}}}
                   :op :set
                   :path [:zeno/sharing]}]})))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Value in a member permissions map must be associative"
         (commands/process-cmds
          {:cmds [{:arg {sg1-id {:zeno/members {member1-id 7}}}
                   :op :set
                   :path [:zeno/sharing]}]})))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Unknown Sharing Group member permission"
         (commands/process-cmds
          {:cmds [{:arg {sg1-id {:zeno/members {member1-id {:zeno/permissions
                                                            #{:not-a-perm}}}}}
                   :op :set
                   :path [:zeno/sharing]}]})))))

(deftest test-sharing-set-sg-id
  (let [*next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        sg1-id (make-id)
        sg2-id (make-id)
        member1-id (make-id)
        member2-id (make-id)
        member3-id (make-id)
        cmds1 [{:arg {sg1-id {:zeno/members {member1-id {:zeno/permissions
                                                         sharing/all-permissions}
                                             member2-id {:zeno/permissions
                                                         #{:zeno/read-data}}}
                              :zeno/paths #{[:a :b]}}}
                :op :set
                :path [:zeno/sharing]}]
        ret1 (commands/process-cmds {:cmds cmds1})
        cmds2 [{:arg {:zeno/members {member3-id {:zeno/permissions
                                                 sharing/all-permissions}}
                      :zeno/paths #{[:a]}}
                :op :set
                :path [:zeno/sharing sg2-id]}]
        ret2 (commands/process-cmds {:cmds cmds2
                                     :sharing-store (:sharing-store ret1)})
        {:keys [sharing-store]} ret2]
    (is (= true (sharing/can-read? {:sharing-store sharing-store
                                    :actor-id member3-id
                                    :path [:a :c]})))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Sharing Group id must be a string"
         (commands/process-cmds
          {:cmds [{:arg {}
                   :op :set
                   :path [:zeno/sharing 7]}]})))))  ; 7 is not a string

(deftest test-sharing-set-paths
  (let [*next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        sg1-id (make-id)
        sg2-id (make-id)
        member1-id (make-id)
        member2-id (make-id)
        cmds1 [{:arg {sg1-id {:zeno/members {member1-id {:zeno/permissions
                                                         sharing/all-permissions}
                                             member2-id {:zeno/permissions
                                                         #{:zeno/read-data}}}
                              :zeno/paths #{[:a :b]}}}
                :op :set
                :path [:zeno/sharing]}]
        ret1 (commands/process-cmds {:cmds cmds1})
        cmds2 [{:arg #{[:c]}
                :op :set
                :path [:zeno/sharing sg1-id :zeno/paths]}]
        ret2 (commands/process-cmds {:cmds cmds2
                                     :sharing-store (:sharing-store ret1)})
        {:keys [sharing-store]} ret2]
    (is (= true (sharing/can-read? {:sharing-store sharing-store
                                    :actor-id member2-id
                                    :path [:c]})))
    (is (= false (sharing/can-write? {:sharing-store sharing-store
                                      :actor-id member2-id
                                      :path [:c]})))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Unknown field"
         (commands/process-cmds
          {:cmds [{:arg {}
                   :op :set
                   :path [:zeno/sharing sg1-id :zeno/not-a-field]}]})))))

(deftest test-sharing-set-members
  (let [*next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        sg1-id (make-id)
        sg2-id (make-id)
        member1-id (make-id)
        member2-id (make-id)
        member3-id (make-id)
        cmds1 [{:arg {sg1-id {:zeno/members {member1-id {:zeno/permissions
                                                         sharing/all-permissions}
                                             member2-id {:zeno/permissions
                                                         #{:zeno/read-data}}}
                              :zeno/paths #{[:a :b]}}}
                :op :set
                :path [:zeno/sharing]}]
        ret1 (commands/process-cmds {:cmds cmds1})
        cmds2 [{:arg {member3-id {:zeno/permissions
                                  #{:zeno/read-data}}}
                :op :set
                :path [:zeno/sharing sg1-id :zeno/members]}]
        ret2 (commands/process-cmds {:cmds cmds2
                                     :sharing-store (:sharing-store ret1)})
        {:keys [sharing-store]} ret2]
    (is (= true (sharing/can-read? {:sharing-store sharing-store
                                    :actor-id member3-id
                                    :path [:a :b]})))
    (is (= false (sharing/can-write? {:sharing-store sharing-store
                                      :actor-id member3-id
                                      :path [:a :b]})))
    ;; Can write to [:c] because it is not shared
    (is (= true (sharing/can-write? {:sharing-store sharing-store
                                     :actor-id member3-id
                                     :path [:c]})))
    (is (= false (sharing/can-write? {:sharing-store sharing-store
                                      :actor-id member1-id
                                      :path [:a :b]})))
    (is (= false (sharing/can-read? {:sharing-store sharing-store
                                     :actor-id member2-id
                                     :path [:a :b]})))))

(deftest test-sharing-set-member-info
  (let [*next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        sg1-id (make-id)
        sg2-id (make-id)
        member1-id (make-id)
        member2-id (make-id)
        member3-id (make-id)
        cmds1 [{:arg {sg1-id {:zeno/members {member1-id {:zeno/permissions
                                                         sharing/all-permissions}
                                             member2-id {:zeno/permissions
                                                         #{:zeno/read-data}}}
                              :zeno/paths #{[:a :b]}}}
                :op :set
                :path [:zeno/sharing]}]
        ret1 (commands/process-cmds {:cmds cmds1})
        cmds2 [{:arg {:zeno/permissions #{:zeno/read-data}}
                :op :set
                :path [:zeno/sharing sg1-id :zeno/members member3-id]}]
        ret2 (commands/process-cmds {:cmds cmds2
                                     :sharing-store (:sharing-store ret1)})
        {:keys [sharing-store]} ret2]
    (is (= true (sharing/can-read? {:sharing-store sharing-store
                                    :actor-id member3-id
                                    :path [:a :b]})))
    (is (= false (sharing/can-write? {:sharing-store sharing-store
                                      :actor-id member3-id
                                      :path [:a :b]})))
    ;; Can write to [:c] because it is not shared
    (is (= true (sharing/can-write? {:sharing-store sharing-store
                                     :actor-id member3-id
                                     :path [:c]})))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Member id must be a string"
         (commands/process-cmds
          {:cmds [{:arg {}
                   :op :set
                   :path  [:zeno/sharing sg1-id :zeno/members 7]}]})))))

(deftest test-sharing-set-member-perms
  (let [*next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        sg1-id (make-id)
        sg2-id (make-id)
        member1-id (make-id)
        member2-id (make-id)
        member3-id (make-id)
        cmds1 [{:arg {sg1-id {:zeno/members {member1-id {:zeno/permissions
                                                         sharing/all-permissions}
                                             member2-id {:zeno/permissions
                                                         #{:zeno/read-data}}}
                              :zeno/paths #{[:a :b]}}}
                :op :set
                :path [:zeno/sharing]}]
        ret1 (commands/process-cmds {:cmds cmds1})
        cmds2 [{:arg #{:zeno/read-data}
                :op :set
                :path [:zeno/sharing sg1-id :zeno/members member3-id
                       :zeno/permissions]}]
        ret2 (commands/process-cmds {:cmds cmds2
                                     :sharing-store (:sharing-store ret1)})
        {:keys [sharing-store]} ret2]
    (is (= true (sharing/can-read? {:sharing-store sharing-store
                                    :actor-id member3-id
                                    :path [:a :b]})))
    (is (= false (sharing/can-write? {:sharing-store sharing-store
                                      :actor-id member3-id
                                      :path [:a :b]})))
    ;; Can write to [:c] because it is not shared
    (is (= true (sharing/can-write? {:sharing-store sharing-store
                                     :actor-id member3-id
                                     :path [:c]})))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Unknown field"
         (commands/process-cmds
          {:cmds [{:arg {}
                   :op :set
                   :path [:zeno/sharing sg1-id :zeno/members member3-id
                          :zeno/not-a-field]}]})))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Sharing group member permissions must be a set"
         (commands/process-cmds
          {:cmds [{:arg :not-a-set
                   :op :set
                   :path [:zeno/sharing sg1-id :zeno/members member3-id
                          :zeno/permissions]}]})))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Unknown Sharing Group member permission"
         (commands/process-cmds
          {:cmds [{:arg #{:not-a-perm}
                   :op :set
                   :path [:zeno/sharing sg1-id :zeno/members member3-id
                          :zeno/permissions]}]})))))

(deftest test-sharing-set-member-perm
  (let [*next-id-num (atom 0)
        make-id #(let [n (swap! *next-id-num inc)]
                   (str "I" n))
        sg1-id (make-id)
        sg2-id (make-id)
        member1-id (make-id)
        member2-id (make-id)
        member3-id (make-id)
        cmds1 [{:arg {sg1-id {:zeno/members {member1-id {:zeno/permissions
                                                         sharing/all-permissions}
                                             member2-id {:zeno/permissions
                                                         #{:zeno/read-data}}}
                              :zeno/paths #{[:a :b]}}}
                :op :set
                :path [:zeno/sharing]}]
        ret1 (commands/process-cmds {:cmds cmds1})
        cmds2 [{:arg true
                :op :set
                :path [:zeno/sharing sg1-id :zeno/members member3-id
                       :zeno/permissions :zeno/read-data]}]
        ret2 (commands/process-cmds {:cmds cmds2
                                     :sharing-store (:sharing-store ret1)})
        {:keys [sharing-store]} ret2]
    (is (= true (sharing/can-read? {:sharing-store sharing-store
                                    :actor-id member3-id
                                    :path [:a :b]})))
    (is (= false (sharing/can-write? {:sharing-store sharing-store
                                      :actor-id member3-id
                                      :path [:a :b]})))
    ;; Can write to [:c] because it is not shared
    (is (= true (sharing/can-write? {:sharing-store sharing-store
                                     :actor-id member3-id
                                     :path [:c]})))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Unknown Sharing Group member permission"
         (commands/process-cmds
          {:cmds [{:arg {}
                   :op :set
                   :path [:zeno/sharing sg1-id :zeno/members member3-id
                          :zeno/permissions :zeno/not-a-perm]}]})))
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"Can't set `:zeno/membership-status` directly."
         (commands/process-cmds
          {:cmds [{:arg :zeno/accepted
                   :op :set
                   :path [:zeno/sharing sg1-id :zeno/members member3-id
                          :zeno/membership-status]}]})))))
