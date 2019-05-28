(ns ns-tracker.dependency
  "Bidirectional graphs of dependencies and dependent objects."
  (:require [clojure.set :refer [union]]
            [clojure.java.io :as io]
            [clojure.edn :as edn]))

(defn graph "Returns a new, empty, dependency graph." []
  {:dependencies {}
   :dependents {}})

(defn seq-union
  "A union that preserves order."
  ([] '())
  ([s1] s1)
  ([s1 s2] (concat s1 (remove (set s1) s2)))
  ([s1 s2 & sets] (reduce seq-union (list* s1 s2 sets))))

(defn- transitive
  "Recursively expands the set of dependency relationships starting
  at (get m x)"
  [m x]
  (reduce (fn [s k]
            (seq-union s (transitive m k)))
          (get m x) (get m x)))

#_(defn- fast-transitive
  [m dep]
  (loop [acc [], already-seen #{}, [subdep & more] (get m dep)]
    (cond
      (not subdep)
      acc

      (contains? already-seen subdep)
      (recur acc already-seen more)

      :else
      (recur
       (conj acc subdep)
       (conj already-seen subdep)
       (filter (complement already-seen)
               (concat more (get m subdep)))))))

(defn- fast-transitive
  [m dep]
  (loop [acc [], already-seen #{}, [subdep & more] (get m dep)]
    (cond
      (not subdep)
      acc

      (contains? already-seen subdep)
      (recur acc already-seen more)

      :else
      (recur
       (conj acc subdep)
       (conj already-seen subdep)
       (concat
        (filter (complement already-seen)
                (get m subdep))
        more)))))

(defn- subdeps [m dep]
  (when-let [deps (seq (get m dep))]
    (reduce (comp distinct concat) deps (for [dep deps]
                                          (distinct (subdeps m dep))))))

(defn dependencies
  "Returns the set of all things x depends on, directly or transitively."
  [graph x]
  (transitive (:dependencies graph) x))

(defn- c []
  (time (transitive (:dependencies @graph*) 'metabase.sample-data)))

(defn- d []
  (time (fast-transitive (:dependencies @graph*) 'metabase.sample-data)))

(defn- e []
  (time (subdeps (:dependencies @graph*) 'metabase.sample-data)))

(defn- cd []
  (let [_ (println "c")
        c (c)
        _ (println "d")
        d (d)
        _ (println "e")
        e (e)]
    (println "(count c):" (count c)) ; NOCOMMIT
    (println "(count d):" (count d)) ; NOCOMMIT
    (println "(count e):" (count e)) ; NOCOMMIT
    (println "= c d" (= c d) "set=" (= (set c) (set d)))
    (println "= c e" (= c e) "set=" (= (set c) (set e)))))

(defn dependents
  "Returns the set of all things which depend upon x, directly or
  transitively."
  [graph x]
  (transitive (:dependents graph) x))

(defn- fast-dependencies [graph x]
  (loop [acc [], already-seen #{}, [dep & more] (get-in graph [:dependencies x]), loops 100]
    (if-not dep
      acc
      (when (pos? loops)
        (recur
         (cond-> acc
           (not (contains? already-seen dep)) (conj dep))
         (conj already-seen dep)
         (filter (complement already-seen)
                 (concat more (get-in graph [:dependencies dep])))
         (dec loops))))))

(defn- a []
  (time (dependencies @graph* 'metabase.sample-data)))

(defn- b []
  (time (fast-dependencies @graph* 'metabase.sample-data)))

(defn depends?
  "True if x is directly or transitively dependent on y."
  [graph x y]
  (some #(= y %) (dependencies graph x)))

(defn fast-depends?
  "Does `x` depend on `y`."
  [graph x y]
  (println "y:" y)                      ; NOCOMMIT
  (loop [[x & more] [x]]
    (println "x:" x) ; NOCOMMIT
    (cond
      (= x y)
      true

      (seq more)
      (recur more)

      :else
      (when-let [deps (get-in graph [:dependencies x])]
        (when (seq deps)
          (recur deps))))))

;; NOCOMMIT
(defn- x []
  (time (depends? @graph* 'metabase.sample-data 'metabase.cmd.refresh-integration-test-db-metadata)))

(defn- y []
  (time (fast-depends? @graph* 'metabase.sample-data 'metabase.cmd.refresh-integration-test-db-metadata)))

(defn dependent
  "True if y is a dependent of x."
  [graph x y]
  (some #(= y %) (dependents graph x)))

(defn- add-relationship [graph key x y]
  (update-in graph [key x] union #{y}))

(defn depend
  "Adds to the dependency graph that x depends on deps. Forbids
  circular and self-referential dependencies."
  ([graph x] graph)

  ([graph x dep]
   (do
     (println (list 'depends? 'graph dep x))
     (assert (not (depends? graph dep x)) "circular dependency")
     (assert (not (= x dep)) "self-referential dependency"))
   (-> graph
       (add-relationship :dependencies x dep)
       (add-relationship :dependents dep x)))

  ([graph x dep & more]
   (reduce (fn [g d]
             (depend g x d))
           graph (cons dep more))))

(defn- remove-from-map [amap x]
  (reduce (fn [m [k vs]]
            (assoc m k (disj vs x)))
          {} (dissoc amap x)))

(defn remove-all
  "Removes all references to x in the dependency graph."
  ([graph] graph)
  ([graph x]
   (assoc graph
     :dependencies (remove-from-map (:dependencies graph) x)
     :dependents (remove-from-map (:dependents graph) x)))
  ([graph x & more]
   (reduce remove-all
           graph (cons x more))))

(defn remove-key
  "Removes the key x from the dependency graph without removing x as a
  depedency of other keys."
  ([graph] graph)
  ([graph x]
   (assoc graph
     :dependencies (dissoc (:dependencies graph) x)))
  ([graph x & more]
   (reduce remove-key
           graph (cons x more))))

(def ^:private graph*
  (delay
   (with-open [reader (java.io.PushbackReader. (io/reader (io/file "/Users/cam/ns-tracker/src/ns_tracker/graph.edn")))]
     (edn/read reader))))

(defn- testt []
  (let [nn    'metabase.cmd.refresh-integration-test-db-metadata
        deps  '#{metabase.sample-data metabase.models.field metabase.db toucan.db clojure.java.io metabase.models.database metabase.sync environ.core}]
    (time
     (apply depend @graph* nn deps))
    nil))
