(ns unit.pub-sub-test
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [clojure.test :refer [are deftest is]]
   [deercreeklabs.async-utils :as au]
   [oncurrent.zeno.client :as zc]
   [oncurrent.zeno.utils :as u]
   [taoensso.timbre :as log])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(deftest test-pub-sub
  (au/test-async
   1000
   (au/go
     (let [zc (zc/zeno-client)
           ch (ca/chan)
           cb #(ca/put! ch %)
           topic "my-num-topic"
           msg "100"
           unsub! (zc/subscribe-to-topic! zc topic cb)]
       (is (ifn? unsub!))
       (is (= nil (zc/publish-to-topic! zc topic msg)))
       (is (= msg (au/<? ch)))
       (is (= true (unsub!)))
       (zc/shutdown! zc)))))

(deftest test-pub-sub-multiple-pubs
  (au/test-async
   1000
   (au/go
     (let [zc (zc/zeno-client)
           ch (ca/chan)
           cb #(ca/put! ch %)
           topic "my-num-topic"
           msg1 "100"
           msg2 "Ignore that last message"
           unsub! (zc/subscribe-to-topic! zc topic cb)]
       (is (ifn? unsub!))
       (is (= nil (zc/publish-to-topic! zc topic msg1)))
       (is (= msg1 (au/<? ch)))
       (is (= nil (zc/publish-to-topic! zc topic msg2)))
       (is (= msg2 (au/<? ch)))
       (is (= true (unsub!)))
       (zc/shutdown! zc)))))

(deftest test-pub-sub-multiple-subs
  (au/test-async
   1000
   (au/go
     (let [zc (zc/zeno-client)
           ch1 (ca/chan)
           ch2 (ca/chan)
           cb1 #(ca/put! ch1 %)
           cb2 #(ca/put! ch2 %)
           topic "my-fav-topic"
           msg "blue is my favorite color"
           unsub1! (zc/subscribe-to-topic! zc topic cb1)
           unsub2! (zc/subscribe-to-topic! zc topic cb2)]
       (is (ifn? unsub1!))
       (is (ifn? unsub2!))
       (is (= nil (zc/publish-to-topic! zc topic msg)))
       (is (= msg (au/<? ch1)))
       (is (= msg (au/<? ch2)))
       (is (= true (unsub1!)))
       (is (= true (unsub2!)))
       (zc/shutdown! zc)))))

(deftest test-pub-sub-multiple-pubs-and-multiple-subs
  (au/test-async
   1000
   (au/go
     (let [zc (zc/zeno-client)
           ch1 (ca/chan)
           ch2 (ca/chan)
           cb1 #(ca/put! ch1 %)
           cb2 #(ca/put! ch2 %)
           topic "my-fav-topic"
           msg1 "AAAAAAA"
           msg2 "BBBBB 23432 XXXX"
           unsub1! (zc/subscribe-to-topic! zc topic cb1)
           unsub2! (zc/subscribe-to-topic! zc topic cb2)]
       (is (ifn? unsub1!))
       (is (ifn? unsub2!))
       (is (= nil (zc/publish-to-topic! zc topic msg1)))
       (is (= msg1 (au/<? ch1)))
       (is (= msg1 (au/<? ch2)))
       (is (= nil (zc/publish-to-topic! zc topic msg2)))
       (is (= msg2 (au/<? ch1)))
       (is (= msg2 (au/<? ch2)))
       (is (= true (unsub1!)))
       (is (= true (unsub2!)))
       (zc/shutdown! zc)))))

;; TODO: Test bad arguments, cbs that throw, etc.
