(ns metabase.driver.impala
  (:require [clojure
             [set :as set]
             [string :as str]]
            [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [honeysql.core :as hsql]
            [java-time :as t]
            [metabase
             [driver :as driver]
             [util :as u]]
            [metabase.db.spec :as dbspec]
            [metabase.driver.common :as driver.common]
            [metabase.driver.sql-jdbc
             [common :as sql-jdbc.common]
             [connection :as sql-jdbc.conn]
             [execute :as sql-jdbc.execute]
             [sync :as sql-jdbc.sync]]
            [metabase.driver.sql.query-processor :as sql.qp]
            [metabase.driver.sql.util.unprepare :as unprepare]
            [metabase.models.table :refer [Table]]
            [metabase.query-processor.timezone :as qp.timezone]
            [metabase.util
             [date-2 :as u.date]
             [honeysql-extensions :as hx]
             [i18n :refer [trs]]
             [ssh :as ssh]]
            [toucan.db :as db])
  (:import [java.sql DatabaseMetaData ResultSet ResultSetMetaData Types]
           [java.time LocalDate LocalDateTime OffsetDateTime OffsetTime ZonedDateTime]))

;;; # IMPLEMENTATION
;; See http://www.cloudera.com/documentation/other/connectors/impala-jdbc/latest/Cloudera-JDBC-Driver-for-Impala-Install-Guide.pdf
;; for all information regarding the Impala JDBC driver.

(driver/register! :impala, :parent :sql-jdbc)


(defmethod sql-jdbc.sync/database-type->base-type :impala
  [_ database-type]
  (condp re-matches (name database-type)
    #"INT"        :type/Integer
    #"STRING"     :type/Text
    #"ARRAY"      :type/Text
    #"BIGINT"     :type/BigInteger
    #"BINARY"     :type/*
    #"BOOLEAN"    :type/Boolean
    #"CHAR"       :type/Text
    #"DATE"       :type/Date
    #"DECIMAL"    :type/Decimal
    #"DOUBLE"     :type/Float
    #"FLOAT"      :type/Float
    #"MAP"        :type/Text
    #"SMALLINT"   :type/Integer
    #"STRUCT"     :type/Text
    #"TIMESTAMP"  :type/DateTime
    #"TINYINT"    :type/Integer
    #"VARCHAR"    :type/Text))



;(defmethod sql-jdbc.sync/active-tables :impala
;  [& args]
;  (apply sql-jdbc.sync/post-filtered-active-tables args))


(defmethod sql-jdbc.conn/connection-details->spec :impala
  [_ {:keys [host port db make-pool? authMech useNative user password connProperties]
      :or {host "localhost", port 21050, db "default", make-pool? true, authMech "0", useNative "1" connProperties ""}
      :as   details}]
  (-> {:classname "com.cloudera.impala.jdbc41.Driver" ; must be in plugins directory
          :subprotocol "impala"
          :subname (str "//" host ":" port "/" db ";AuthMech=" authMech ";UID=" user ";PWD=" password ";UseNativeQuery=" useNative ";" connProperties)  ;;Use UseNativeQuery=1 to prevent SQL rewriting by the JDBC driver
          :make-pool? make-pool?}
          (set/rename-keys {:dbname :db})
          (dissoc details :host :port :db :connProperties)
      (sql-jdbc.common/handle-additional-options details)))

(defmethod sql-jdbc.sync/active-tables :mysql
  [& args]
  (apply sql-jdbc.sync/post-filtered-active-tables args))

(defmethod sql.qp/quote-style :impala [_] :mysql)

(defn- trunc
  "Truncate a datetime, also see:
   https://www.cloudera.com/documentation/enterprise/5-8-x/topics/impala_datetime_functions.html

      (trunc :day v) -> TRUNC(v, 'day')"
  [format-template v]
  (hsql/call :trunc v (hx/literal format-template)))

(defn- extract-old
  "Extract value from datetime field), also see:
   https://www.cloudera.com/documentation/enterprise/5-8-x/topics/impala_datetime_functions.html

      (extract :day v) -> extract(v, 'dd')"
  [format-template v]
  (hsql/call :extract v (hx/literal format-template)))

(defmethod driver/humanize-connection-error-message :impala
  [_ message]
  (condp re-matches message
    #"^Communications link failure\s+The last packet sent successfully to the server was 0 milliseconds ago. The driver has not received any packets from the server.$"
    (driver.common/connection-error-messages :cannot-connect-check-host-and-port)

    #"^Unknown database .*$"
    (driver.common/connection-error-messages :database-name-incorrect)

    #"Access denied for user.*$"
    (driver.common/connection-error-messages :username-or-password-incorrect)

    #"Must specify port after ':' in connection string"
    (driver.common/connection-error-messages :invalid-hostname)

    #".*"                               ; default
    message))

(defn- string-length-fn [field-key]
  (hsql/call :char_length field-key))

(defmethod sql.qp/current-datetime-honeysql-form :impala [_] :%now)

(defmethod sql.qp/unix-timestamp->honeysql [:impala :seconds]
  [_ _ expr]
  (hx/->timestamp (hsql/call :from_unixtime expr)))

(defn- date-format [format-str expr]
  (hsql/call :date_format expr (hx/literal format-str)))

(defn- str-to-date [format-str expr]
  (hx/->timestamp
   (hsql/call :from_unixtime
              (hsql/call :unix_timestamp
                         expr (hx/literal format-str)))))

(defn- trunc-with-format [format-str expr]
  (str-to-date format-str (date-format format-str expr)))

(defmethod sql.qp/date [:impala :default]         [_ _ expr] (hx/->timestamp expr))
(defmethod sql.qp/date [:impala :minute]          [_ _ expr] (trunc :MI expr))
(defmethod sql.qp/date [:impala :minute-of-hour]  [_ _ expr] (hsql/call :minute (hx/->timestamp expr)))
(defmethod sql.qp/date [:impala :hour]            [_ _ expr] (trunc :HH expr)) 
(defmethod sql.qp/date [:impala :hour-of-day]     [_ _ expr] (hsql/call :hour (hx/->timestamp expr)))
(defmethod sql.qp/date [:impala :day]             [_ _ expr] (trunc :dd expr)) 
(defmethod sql.qp/date [:impala :day-of-month]    [_ _ expr] (hsql/call :dayofmonth (hx/->timestamp expr)))
(defmethod sql.qp/date [:impala :day-of-year]     [_ _ expr] (hsql/call :dayofyear expr)) 
(defmethod sql.qp/date [:impala :week-of-year]    [_ _ expr] (hsql/call :weekofyear (hx/->timestamp expr)))
(defmethod sql.qp/date [:impala :month]           [_ _ expr] (trunc :month expr)) 
(defmethod sql.qp/date [:impala :month-of-year]   [_ _ expr] (hsql/call :month (hx/->timestamp expr)))
(defmethod sql.qp/date [:impala :quarter]         [_ _ expr] (trunc :Q expr)) 
(defmethod sql.qp/date [:impala :year]            [_ _ expr] (hsql/call :trunc (hx/->timestamp expr) (hx/literal :year)))
(defmethod sql.qp/date [:impala :day-of-week]     [_ _ expr] (hsql/call :dayofweek expr))
(defmethod sql.qp/date [:impala :week]            [_ _ expr] (trunc :day expr)) 

(defmethod sql.qp/date [:impala :quarter-of-year] 
  [_ _ expr]
  (hx// (hx/+ (hsql/call :extract :month expr)
                                   2)
                             3))

(defmethod sql.qp/->honeysql [:impala :replace]
  [driver [_ arg pattern replacement]]
  (hsql/call :regexp_replace (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver pattern) (sql.qp/->honeysql driver replacement)))

(defmethod sql.qp/->honeysql [:impala :regex-match-first]
  [driver [_ arg pattern]]
  (hsql/call :regexp_extract (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver pattern)))

(defmethod sql.qp/->honeysql [:impala :median]
  [driver [_ arg]]
  (hsql/call :percentile (sql.qp/->honeysql driver arg) 0.5))

(defmethod sql.qp/->honeysql [:impala :percentile]
  [driver [_ arg p]]
  (hsql/call :percentile (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver p)))

(defmethod sql.qp/add-interval-honeysql-form :impala
  [_ hsql-form amount unit]
  (hx/+ (hx/->timestamp hsql-form) (hsql/raw (format "INTERVAL %d %s" (int amount) (name unit)))))

;; ignore the schema when producing the identifier
(defn qualified-name-components
  "Return the pieces that represent a path to `field`, of the form `[table-name parent-fields-name* field-name]`."
  [{field-name :name, table-id :table_id}]
  [(db/select-one-field :name Table, :id table-id) field-name])

(defmethod sql.qp/field->identifier :impala
  [_ field]
  (apply hsql/qualify (qualified-name-components field)))

(defmethod unprepare/unprepare-value [:impala String]
  [_ value]
  (str \' (str/replace value "'" "\\\\'") \'))

;; Hive/Spark SQL doesn't seem to like DATEs so convert it to a DATETIME first
(defmethod unprepare/unprepare-value [:impala LocalDate]
  [driver t]
  (unprepare/unprepare-value driver (t/local-date-time t (t/local-time 0))))

(defmethod unprepare/unprepare-value [:impala OffsetDateTime]
  [_ t]
  (format "to_utc_timestamp('%s', '%s')" (u.date/format-sql (t/local-date-time t)) (t/zone-offset t)))

(defmethod unprepare/unprepare-value [:impala ZonedDateTime]
  [_ t]
  (format "to_utc_timestamp('%s', '%s')" (u.date/format-sql (t/local-date-time t)) (t/zone-id t)))

;; Hive/Spark SQL doesn't seem to like DATEs so convert it to a DATETIME first
(defmethod sql-jdbc.execute/set-parameter [:impala LocalDate]
  [driver ps i t]
  (sql-jdbc.execute/set-parameter driver ps i (t/local-date-time t (t/local-time 0))))

;; TIMEZONE FIXME — not sure what timezone the results actually come back as
(defmethod sql-jdbc.execute/read-column-thunk [:impala Types/TIME]
  [_ ^ResultSet rs rsmeta ^Integer i]
  (fn []
    (when-let [t (.getTimestamp rs i)]
      (t/offset-time (t/local-time t) (t/zone-offset 0)))))

(defmethod sql-jdbc.execute/read-column-thunk [:impala Types/DATE]
  [_ ^ResultSet rs rsmeta ^Integer i]
  (fn []
    (when-let [t (.getDate rs i)]
      (t/zoned-date-time (t/local-date t) (t/local-time 0) (t/zone-id "UTC")))))

(defmethod sql-jdbc.execute/read-column-thunk [:impala Types/TIMESTAMP]
  [_ ^ResultSet rs rsmeta ^Integer i]
  (fn []
    (when-let [t (.getTimestamp rs i)]
      (t/zoned-date-time (t/local-date-time t) (t/zone-id "UTC")))))