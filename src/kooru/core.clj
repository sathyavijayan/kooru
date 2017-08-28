(ns kooru.core
  (:require [clojure.core.async
             :refer [close! tap >!! <! mult go-loop thread timeout alts!! chan]
             :as a]
            [clojure.set :refer [map-invert]]
            [clojure.tools.logging :as log])
  (:gen-class))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                        ---==| C O N T R O L |==----                        ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn ctrl
  []
  (let [c (chan)]
    {:control-chan c :ground (mult c)}))



(defn abort
  [{:keys [control-chan] :as ctrl}]
  (close! control-chan))



(defn listen
  [{:keys [ground] :as ctrl} c]
  (tap ground c))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;             ---==| A B S T R A C T   C O M P O N E N T |==----             ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn component*
  "Abstract many-to-one component.
  `id`      - an identifier for this component (used in logs etc).
  `inputs`  - map containining input channels keyed by name
  `f`       - function of arity 3 [state input msg].
              `state`  - persistent state of this component. Set to nil for the
                         first time.
              `input`  - name of the channel from which the message was read.
              `msg`    - message read from the input channels.
              f must return a 3-tuple [state' output msg']
              `state'` - updated state.
              `output` - name of the output channel to which msg' prime must
                         be sent to.
              `msg'    - output message
  `outputs` - map containing output channels keyed by name.
  `ctrl`    - ctrl provides mechanism necessary to abort incase of an
              exception and also listen to abort signal from other components
              in the pipeline this component is a part of.

  shutdown behaviour - if all the input channels are closed, the component
  gracefully shuts down by closing all the output channels, triggering a
  cascading shutdown of downstream components of the pipeline."
  [id inputs f outputs ctrl]
  (let [abort-chan  (listen ctrl (chan))]
    (a/thread
      (try
        (let [input-chans (->> inputs vals (into #{abort-chan}))
              inputs'     (map-invert inputs)
              outputs'    (map-invert outputs)]
          (loop [input-chans input-chans
                 state       nil]
            (let [[v p] (alts!! (vec input-chans))]
              (cond
                ;; on abort, just exit
                (= p abort-chan)
                (log/info "Abort received. Shutting down :" id)

                ;; remove any inputs that were closed.
                (nil? v)
                (let [i (disj input-chans p)]
                  (if-not (= i #{abort-chan})
                    (recur i state)
                    (do
                      (->> outputs vals (map close!) doall)
                      (log/info "No more channels to listen to. Shutting down."))))

                ;; call f with the message received.
                :else
                (let [[state' output msg']
                      (f state (get inputs' p) v)
                      out-chan (get outputs output)]
                  (when (and output msg')
                    (>!! out-chan msg'))

                  (recur input-chans state'))))))

        (catch Throwable t
          (log/error t "Error in component :" id ". Aborting.")
          (abort ctrl))))


    {:id id :inputs inputs :outputs outputs}))




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                     ---==| C O M P O N E N T S |==----                     ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn many-to-one
  "many-to-one component - reads messages from many channels, calls function f
  with the message, and routes the output messate to a single output channel.

  `id`      - an identifier for this component (used in logs etc).
  `inputs`  - map containining input channels keyed by name
  `f`       - function of arity 3 [state input msg].
              `state`  - persistent state of this component. Set to nil for the
                         first time.
              `input`  - name of the channel from which the message was read.
              `msg`    - message read from the input channels.
              f must return a tuple [state' msg']
              `state'` - updated state.
              `msg'    - output message
  `output`  - output channel.
  `ctrl`    - ctrl provides mechanism necessary to abort incase of an
              exception and also listen to abort signal from other components
              in the pipeline this component is a part of.
   `f` must be a sing'"
  [id inputs f output ctrl]
  (component*
   id
   inputs
   (fn [state input msg]
     (let [[state' msg'] (f state input msg)]
       [state' :default msg']))
   {:default output}
   ctrl)

  {:id id :inputs inputs :output output})



(defn one-to-one
  "one-to-one component - reads messages from a single  input channel, calls
  function f with the message, and routes the output messate to a single output
  channel.

  `id`      - an identifier for this component (used in logs etc).
  `input`   - input channel
  `f`       - function of arity 2 [state msg].
              `state`  - persistent state of this component. Set to nil for the
                         first time.
              `msg`    - message read from the input channels.
              f must return a tuple [state' msg']
              `state'` - updated state.
              `msg'    - output message
  `output`  - output channel.
  `ctrl`    - ctrl provides mechanism necessary to abort incase of an
              exception and also listen to abort signal from other components
              in the pipeline this component is a part of.
   `f` must be a sing'"
  [id input f output ctrl]
  (component*
   id
   {:default input}
   (fn [state input msg]
     (let [[state' msg'] (f state :default msg)]
       [state' :default msg']))
   {:default output}
   ctrl)

  {:id id :input input :output output})




(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))


(comment


  (defn printer
    [c m]
    (go-loop []
      (if-let [msg (<! c)]
        (do (prn m msg) (recur))
        (prn "PRINTER: " m " SHUTDOWN!!"))))



  (defn sorting-hat
    ([s m]
     [s (-> (Math/abs (.hashCode m))
            (mod 4)
            ((partial nth
                [:gryffindor :hufflepuff :ravenclaw :slytherin])))])

    ([s i m]
     (let [house  (-> (Math/abs (.hashCode m))
                      (mod 4)
                      ((partial nth
                          [:gryffindor :hufflepuff :ravenclaw :slytherin])))]
       [s house m])))


  ;;;;;;;;; abstract component
  (def _ctrl (ctrl))

  (def inputs {:one (chan) :two (chan)})

  (def outputs
    {:gryffindor (chan) :hufflepuff (chan) :ravenclaw (chan) :slytherin (chan)})

  (doseq [[h c] outputs]
    (printer c (str h ">")))

  (component* "sorting-hat" inputs sorting-hat outputs _ctrl)

  (doall (map close! (vals outputs)))

  (>!! (:one inputs) "Peter Parker")

  (>!! (:two inputs) "Harry Potter")

  (close! (:two inputs))

  (close! (:one inputs))

  (abort _ctrl)

  ;;;;;;;;;; many-to-one
  (def output (chan))

  (many-to-one
   "sorting-hat"
   inputs
   (fn [state m]
     [state
      (-> (Math/abs (.hashCode m))
          (mod 4)
          ((partial nth [:gryffindor :hufflepuff :ravenclaw :slytherin])))])
   output
   _ctrl)

  (printer output "SORTING HAT:")




  ;;END REPL
  )
