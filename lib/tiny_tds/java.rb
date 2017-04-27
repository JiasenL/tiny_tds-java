require "tiny_tds/java/version"
require "tiny_tds/sqljdbc4-4.0.jar"

module TinyTds
  module Java
    class DatabaseError < StandardError; end

    OPTS = {}.freeze

    # Mutex used to protect mutable data structures
    @data_mutex = Mutex.new

    # Unless in single threaded mode, protects access to any mutable
    # global data structure in Sequel.
    # Uses a non-reentrant mutex, so calling code should be careful.
    def self.synchronize(&block)
      @data_mutex.synchronize(&block)
    end

    # Make it accesing the java.lang hierarchy more ruby friendly.
    module JavaLang
      include_package 'java.lang'
    end
    
    # Make it accesing the java.sql hierarchy more ruby friendly.
    module JavaSQL
      include_package 'java.sql'
    end

    # Make it accesing the javax.naming hierarchy more ruby friendly.
    module JavaxNaming
      include_package 'javax.naming'
    end

    # Used to identify a jndi connection and to extract the jndi
    # resource name.
    JNDI_URI_REGEXP = /\Ajdbc:jndi:(.+)/
    
    # logging
    class Database
      # ---------------------
      # :section: 6 - Methods relating to logging
      # This methods affect relating to the logging of executed SQL.
      # ---------------------

      # Numeric specifying the duration beyond which queries are logged at warn
      # level instead of info level.
      attr_accessor :log_warn_duration

      # Array of SQL loggers to use for this database.
      attr_accessor :loggers
      
      # Whether to include information about the connection in use when logging queries.
      attr_accessor :log_connection_info
      
      # Log level at which to log SQL queries.  This is actually the method
      # sent to the logger, so it should be the method name symbol. The default
      # is :info, it can be set to :debug to log at DEBUG level.
      attr_accessor :sql_log_level

      # Log a message at error level, with information about the exception.
      def log_exception(exception, message)
        log_each(:error, "#{exception.class}: #{exception.message.strip if exception.message}: #{message}")
      end

      # Log a message at level info to all loggers.
      def log_info(message, args=nil)
        log_each(:info, args ? "#{message}; #{args.inspect}" : message)
      end

      # Yield to the block, logging any errors at error level to all loggers,
      # and all other queries with the duration at warn or info level.
      def log_yield(sql, args=nil, &block)
        log_connection_yield(sql, nil, args, &block)
      end

      # Yield to the block, logging any errors at error level to all loggers,
      # and all other queries with the duration at warn or info level.
      def log_connection_yield(sql, conn, args=nil)
        return yield if @loggers.nil? || @loggers.empty?
        sql = "#{connection_info(conn) if conn && log_connection_info}#{sql}#{"; #{args.inspect}" if args}"
        start = Time.now
        begin
          yield
        rescue => e
          log_exception(e, sql)
          raise
        ensure
          log_duration(Time.now - start, sql) unless e
        end
      end

      # Remove any existing loggers and just use the given logger:
      #
      #   DB.logger = Logger.new($stdout)
      def logger=(logger)
        @loggers = Array(logger)
      end

      private

      # String including information about the connection, for use when logging
      # connection info.
      def connection_info(conn)
        "(conn: #{conn.__id__}) "
      end
      
      # Log message with message prefixed by duration at info level, or
      # warn level if duration is greater than log_warn_duration.
      def log_duration(duration, message)
        log_each((lwd = log_warn_duration and duration >= lwd) ? :warn : sql_log_level, "(#{sprintf('%0.6fs', duration)}) #{message}")
      end

      # Log message at level (which should be :error, :warn, or :info)
      # to all loggers.
      def log_each(level, message)
        @loggers.each{|logger| logger.send(level, message)}
      end
    end

    # connection
    class Database
      # The Java database driver we are using (should be a Java class)
      attr_reader :driver

      # The fetch size to use for JDBC Statement objects created by this database.
      # By default, this is nil so a fetch size is not set explicitly.
      attr_accessor :fetch_size

      attr_reader :opts

      attr_reader :conn

      # Connects to a database.  See Sequel.connect.
      def self.connect(conn_string, opts = OPTS)
        opts = {:uri=>conn_string}.merge!(opts)
        # process opts a bit
        opts = opts.inject({}) do |m, (k,v)|
          k = :user if k.to_s == 'username'
          m[k.to_sym] = v
          m
        end
        db = self.new(opts)
        db.connect(opts)
        db
      end

      def initialize(opts)
        @opts = opts
        @loggers = Array(@opts[:logger]) + Array(@opts[:loggers])
        @sql_log_level = @opts[:sql_log_level] ? @opts[:sql_log_level].to_sym : :info
        @default_dataset = TinyTds::Java::Dataset.new(self)
        @driver = com.microsoft.sqlserver.jdbc.SQLServerDriver
        @fetch_size = nil
        self.database_timezone = opts[:database_timezone]
        raise(Error, "No connection string specified") unless uri
      end

      # Remove any existing loggers and just use the given logger:
      #
      #   DB.logger = Logger.new($stdout)
      def logger=(logger)
        @loggers = Array(logger)
      end

      def database_timezone
        opts[:database_timezone]
      end

      def database_timezone=(zone)
        zone = (zone == :local ? :local : :utc)
        opts[:database_timezone] = zone
      end

      # Execute the given stored procedure with the give name. If a block is
      # given, the stored procedure should return rows.
      def call_sproc(name, opts = OPTS)
        args = opts[:args] || []
        sql = "{call #{name}(#{args.map{'?'}.join(',')})}"
        TinyTds::Java.synchronize do
          cps = conn.prepareCall(sql)

          i = 0
          args.each{|arg| set_ps_arg(cps, arg, i+=1)}

          begin
            if block_given?
              yield log_connection_yield(sql, conn){cps.executeQuery}
            else
              case opts[:type]
              when :insert
                log_connection_yield(sql, conn){cps.executeUpdate}
                last_insert_id(conn, opts)
              else
                log_connection_yield(sql, conn){cps.executeUpdate}
              end
            end
          rescue NativeException, JavaSQL::SQLException => e
            raise_error(e)
          ensure
            cps.close
          end
        end
      end
         
      # Connect to the database using JavaSQL::DriverManager.getConnection.
      def connect(opts)
        conn = if jndi?
          get_connection_from_jndi
        else
          args = [uri(opts)]
          args.concat([opts[:user], opts[:password]]) if opts[:user] && opts[:password]
          begin
            JavaSQL::DriverManager.setLoginTimeout(opts[:login_timeout]) if opts[:login_timeout]
            raise StandardError, "skipping regular connection" if opts[:jdbc_properties]
            JavaSQL::DriverManager.getConnection(*args)
          rescue JavaSQL::SQLException, NativeException, StandardError => e
            raise e unless driver
            # If the DriverManager can't get the connection - use the connect
            # method of the driver. (This happens under Tomcat for instance)
            props = java.util.Properties.new
            if opts && opts[:user] && opts[:password]
              props.setProperty("user", opts[:user])
              props.setProperty("password", opts[:password])
            end
            opts[:jdbc_properties].each{|k,v| props.setProperty(k.to_s, v)} if opts[:jdbc_properties]
            begin
              c = driver.new.connect(args[0], props)
              raise(TinyTds::Java::DatabaseError, 'driver.new.connect returned nil: probably bad JDBC connection string') unless c
              c
            rescue JavaSQL::SQLException, NativeException, StandardError => e2
              if e2.respond_to?(:message=) && e2.message != e.message
                e2.message = "#{e2.message}\n#{e.class.name}: #{e.message}"
              end
              raise e2
            end
          end
        end

        @conn = conn
      end

      def disconnect
        conn.close
      end      

      # Execute the given SQL.  If a block is given, if should be a SELECT
      # statement or something else that returns rows.
      def execute(sql, opts=OPTS, &block)
        return call_sproc(sql, opts, &block) if opts[:sproc]

        TinyTds::Java.synchronize do
          statement(conn) do |stmt|
            if block
              if size = fetch_size
                stmt.setFetchSize(size)
              end
              yield log_connection_yield(sql, conn){stmt.executeQuery(sql)}
            else
              case opts[:type]
              when :ddl
                log_connection_yield(sql, conn){stmt.execute(sql)}
              when :insert
                log_connection_yield(sql, conn){execute_statement_insert(stmt, sql)}
                last_insert_id(conn, Hash[opts].merge!(:stmt=>stmt))
              else
                log_connection_yield(sql, conn){stmt.executeUpdate(sql)}
              end
            end
          end
        end
      end

      # Execute the given DDL SQL, which should not return any
      # values or rows.
      def execute_ddl(sql, opts=OPTS)
        opts = Hash[opts]
        opts[:type] = :ddl
        execute(sql, opts)
      end
      
      # Execute the given INSERT SQL, returning the last inserted
      # row id.
      def execute_insert(sql, opts=OPTS)
        opts = Hash[opts]
        opts[:type] = :insert
        execute(sql, opts)
      end

      # Whether or not JNDI is being used for this connection.
      def jndi?
        !!(uri =~ JNDI_URI_REGEXP)
      end

      # The uri for this connection.  You can specify the uri
      # using the :uri, :url, or :database options.  You don't
      # need to worry about this if you use Sequel.connect
      # with the JDBC connectrion strings.
      def uri(opts=OPTS)
        opts = @opts.merge(opts)
        ur = opts[:uri] || opts[:url] || opts[:database]
        ur =~ /^\Ajdbc:/ ? ur : "jdbc:#{ur}"
      end

      # Returns a dataset for the database. The first argument has to be a string
      # calls Database#fetch and returns a dataset for arbitrary SQL.
      #
      # If a block is given, it is used to iterate over the records:
      #
      #   DB.fetch('SELECT * FROM items') {|r| p r}
      #
      # The +fetch+ method returns a dataset instance:
      #
      #   DB.fetch('SELECT * FROM items').all
      #
      # Options can be given as a second argument: {as: :array} to fetch the dataset as an array.
      def fetch(sql, opts=OPTS, &block)
        ds = @default_dataset.with_sql(sql, self.opts.merge(opts))
        ds.each(&block) if block
        ds
      end
  
      # Runs the supplied SQL statement string on the database server. Returns nil.
      #
      #   DB.run("SET some_server_variable = 42")
      def run(sql)
        execute_ddl(sql)
        nil
      end

      private
         
      # Execute the insert SQL using the statement
      def execute_statement_insert(stmt, sql)
        stmt.executeUpdate(sql)
      end

      # Gets the connection from JNDI.
      def get_connection_from_jndi
        jndi_name = JNDI_URI_REGEXP.match(uri)[1]
        JavaxNaming::InitialContext.new.lookup(jndi_name).connection
      end
            
      # Gets the JDBC connection uri from the JNDI resource.
      def get_uri_from_jndi
        conn = get_connection_from_jndi
        conn.meta_data.url
      ensure
        conn.close if conn
      end

      # Get the last inserted id using SCOPE_IDENTITY().
      def last_insert_id(conn, opts=OPTS)
        statement(conn) do |stmt|
          sql = opts[:prepared] ? ATAT_IDENTITY : SCOPE_IDENTITY
          rs = log_connection_yield(sql, conn){stmt.executeQuery(sql)}
          rs.next
          rs.getLong(1)
        end
      end

      # Java being java, you need to specify the type of each argument
      # for the prepared statement, and bind it individually.  This
      # guesses which JDBC method to use, and hopefully JRuby will convert
      # things properly for us.
      def set_ps_arg(cps, arg, i)
        case arg
        when Integer
          cps.setLong(i, arg)
        when Sequel::SQL::Blob
          cps.setBytes(i, arg.to_java_bytes)
        when String
          cps.setString(i, arg)
        when Float
          cps.setDouble(i, arg)
        when TrueClass, FalseClass
          cps.setBoolean(i, arg)
        when NilClass
          set_ps_arg_nil(cps, i)
        when DateTime
          cps.setTimestamp(i, java_sql_datetime(arg))
        when Date
          cps.setDate(i, java_sql_date(arg))
        when Time
          cps.setTimestamp(i, java_sql_timestamp(arg))
        when Java::JavaSql::Timestamp
          cps.setTimestamp(i, arg)
        when Java::JavaSql::Date
          cps.setDate(i, arg)
        else
          cps.setObject(i, arg)
        end
      end

      # Use setString with a nil value by default, but this doesn't work on all subadapters.
      def set_ps_arg_nil(cps, i)
        cps.setString(i, nil)
      end
      
      # Yield a new statement object, and ensure that it is closed before returning.
      def statement(conn)
        stmt = conn.createStatement
        yield stmt
      rescue NativeException, JavaSQL::SQLException => e
        raise e
      ensure
        stmt.close if stmt
      end
    end

    class Dataset
      include Enumerable

      OPTS = {}.freeze

      # Unless in single threaded mode, protects access to any mutable
      # global data structure in Sequel.
      # Uses a non-reentrant mutex, so calling code should be careful.
      def synchronize(&block)
        @cache_mutex.synchronize(&block)
      end

      # The database related to this dataset. This is the Database instance that
      # will execute all of this dataset's queries.
      attr_reader :db

      # The hash of options for this dataset, keys are symbols.
      attr_reader :opts

      # Constructs a new Dataset instance with an associated database.
      # Datasets are usually constructed by invoking the Database#fetch method:
      #
      #   DB.fetch('SELECT 1=1 AS a')
      def initialize(db)
        @db = db
        @opts = {}
        @cache_mutex = Mutex.new
        @cache = {}
      end

      # Define a hash value such that datasets with the same class, DB, and opts
      # will be considered equal.
      def ==(o)
        o.is_a?(self.class) && db == o.db && opts == o.opts
      end

      # Alias for ==
      def eql?(o)
        self == o
      end

      # Define a hash value such that datasets with the same class, DB, and opts,
      # will have the same hash value.
      def hash
        [self.class, db, opts].hash
      end
      
      # Returns a string representation of the dataset including the class name 
      # and the corresponding SQL select statement.
      def inspect
        "#<#{self.class.name}: #{sql.inspect}>"
      end

      def sql
        opts[:sql]
      end

      # The dataset options that require the removal of cached columns
      # if changed.
      COLUMN_CHANGE_OPTS = [:sql].freeze

      TRUE_FREEZE = RUBY_VERSION >= '2.4'

      # On Ruby 2.4+, use clone(:freeze=>false) to create clones, because
      # we use true freezing in that case, and we need to modify the opts
      # in the frozen copy.
      #
      # On Ruby <2.4, just use Object#clone directly, since we don't
      # use true freezing as it isn't possible.
      if TRUE_FREEZE
        # Save original clone implementation, as some other methods need
        # to call it internally.
        alias _clone clone
        private :_clone

        # Returns a new clone of the dataset with the given options merged.
        # If the options changed include options in COLUMN_CHANGE_OPTS, the cached
        # columns are deleted.  This method should generally not be called
        # directly by user code.
        def clone(opts = OPTS)
          c = super(:freeze=>false)
          c.opts.merge!(opts)
          unless opts.each_key{|o| break if COLUMN_CHANGE_OPTS.include?(o)}
            c.clear_columns_cache
          end
          c.freeze if frozen?
          c
        end
      else
        # :nocov:
        def clone(opts = OPTS) # :nodoc:
          c = super()
          c.opts.merge!(opts)
          unless opts.each_key{|o| break if COLUMN_CHANGE_OPTS.include?(o)}
            c.clear_columns_cache
          end
          c.freeze if frozen?
          c
        end
        # :nocov:
      end

      # Set the db, opts, and cache for the copy of the dataset.
      def initialize_copy(c)
        @db = c.db
        @opts = Hash[c.opts]
        if cols = c.cache_get(:_columns)
          @cache = {:_columns=>cols}
        else
          @cache = {}
        end
      end

      def with_sql(sql, opts=OPTS)
        clone(opts.merge(:sql => sql))
      end

      def each(options=OPTS)
        fetch_rows(opts[:sql], opts.merge(options)){|r| yield r}
        self
      end

      alias :all :entries

      # Correctly return rows from the database and return them as hashes.
      def fetch_rows(sql, opts=OPTS, &block)
        db.execute(sql){|result| process_result_set(result, opts, &block)}
        self
      end

      # Split out from fetch rows to allow processing of JDBC result sets
      # that don't come from issuing an SQL string.
      def process_result_set(result, opts=OPTS)
        meta = result.getMetaData
        if fetch_size = opts[:fetch_size]
          result.setFetchSize(fetch_size)
        end

        converters = []
        self.columns = meta.getColumnCount.times.map do |i|
          col = i + 1
          converters << TypeConverter::MAP[meta.getColumnType(col)]
          meta.getColumnLabel(col)
        end

        fetch_as_array = opts[:as] == :array
        while result.next
          row = fetch_as_array ? [] : {}
          _columns.each_with_index do |column, i|
            k = fetch_as_array ? i : column
            col = i+1
            row[k] = converters[i].call(result, col, opts)
          end
          yield row
        end
      ensure
        result.close
      end

      # Returns the columns in the result set in order as an array of symbols.
      # If the columns are currently cached, returns the cached value. Otherwise,
      # a SELECT query is performed to retrieve a single row in order to get the columns.
      #
      #   DB.fetch('SELECT 1 AS a, 2 AS b').columns
      #   # => [:a, :b]
      def columns
        _columns || columns!
      end

      # Ignore any cached column information and perform a query to retrieve
      # a row in order to get the columns.
      #
      #   dataset = DB.fetch('SELECT 1 AS a, 2 AS b')
      #   dataset.columns!
      #   # => [:a, :b]
      def columns!
        ds = clone(opts.merge(:sql => "SELECT TOP 1 [T1].* FROM (#{opts[:sql]}) \"T1\""))
        ds.each{break}

        if cols = ds.cache[:_columns]
          self.columns = cols
        else
          []
        end
      end

      protected

      # Access the cache for the current dataset.  Should be used with caution,
      # as access to the cache is not thread safe without a mutex if other
      # threads can reference the dataset.  Symbol keys prefixed with an
      # underscore are reserved for internal use.
      attr_reader :cache

      # Retreive a value from the dataset's cache in a thread safe manner.
      def cache_get(k)
        synchronize{@cache[k]}
      end

      # Set a value in the dataset's cache in a thread safe manner.
      def cache_set(k, v)
        synchronize{@cache[k] = v}
      end

      # Clear the columns hash for the current dataset.  This is not a
      # thread safe operation, so it should only be used if the dataset
      # could not be used by another thread (such as one that was just
      # created via clone).
      def clear_columns_cache
        @cache.delete(:_columns)
      end

      # The cached columns for the current dataset.
      def _columns
        cache_get(:_columns)
      end

      private

      # Set the columns for the current dataset.
      def columns=(v)
        cache_set(:_columns, v)
      end
    end

    class TypeConverter
      OPTS = {}.freeze      

      %w'Boolean Float Double Int Long Short'.each do |meth|
        class_eval("def #{meth}(r, i, opts=OPTS) v = r.get#{meth}(i); v unless r.wasNull end", __FILE__, __LINE__)
      end
      %w'Object Array String Bytes'.each do |meth|
        class_eval("def #{meth}(r, i, opts=OPTS) r.get#{meth}(i) end", __FILE__, __LINE__)
      end
      def RubyTime(r, i, opts=OPTS)
        if v = r.getString(i)
          Time.parse(v+(opts[:database_timezone] == :utc ? 'UTC' : ''))
        end
      end
      def RubyDate(r, i, opts=OPTS)
        if v = r.getDate(i)
          Date.civil(v.getYear + 1900, v.getMonth + 1, v.getDate)
        end
      end
      def RubyTimestamp(r, i, opts=OPTS)
        if v = r.getTimestamp(i)
          timezone = opts[:database_timezone] == :local ? :local : :utc
          Time.send(timezone, v.getYear + 1900, v.getMonth + 1, v.getDate, v.getHours, v.getMinutes, v.getSeconds, v.getNanos)
        end
      end
      def RubyDateTimeOffset(r, i, opts=OPTS)
        if v = r.getDateTimeOffset(i)
          Time.parse(v.to_s)
        end
      end
      def RubyBigDecimal(r, i, opts=OPTS)
        if v = r.getBigDecimal(i)
          BigDecimal.new(v.to_string)
        end
      end
      def RubyBlob(r, i, opts=OPTS)
        if v = r.getBytes(i)
          String.from_java_bytes(v)
        end
      end
      def RubyClob(r, i, opts=OPTS)
        if v = r.getClob(i)
          v.getSubString(1, v.length)
        end
      end

      INSTANCE = new
      o = INSTANCE
      MAP = Hash.new(o.method(:Object))
      types = Java::JavaSQL::Types

      {
        :ARRAY => :Array,
        :BOOLEAN => :Boolean,
        :CHAR => :String,
        :DOUBLE => :Double,
        :FLOAT => :Double,
        :INTEGER => :Int,
        :LONGNVARCHAR => :String,
        :LONGVARCHAR => :String,
        :NCHAR => :String,
        :REAL => :Float,
        :SMALLINT => :Short,
        :TINYINT => :Short,
        :VARCHAR => :String,
      }.each do |type, meth|
        MAP[types.const_get(type)] = o.method(meth) 
      end

      {
        :BINARY => :Blob,
        :BLOB => :Blob,
        :CLOB => :Clob,
        :DATE => :Date,
        :DECIMAL => :BigDecimal,
        :LONGVARBINARY => :Blob,
        :NCLOB => :Clob,
        :NUMERIC => :BigDecimal,
        :TIME => :Time,
        :TIMESTAMP => :Timestamp,
        :VARBINARY => :Blob,
      }.each do |type, meth|
        MAP[types.const_get(type)] = o.method(:"Ruby#{meth}") 
      end

      MAP[::Java::MicrosoftSql::Types::DATETIMEOFFSET] = o.method(:RubyDateTimeOffset)

      MAP.freeze
      INSTANCE.freeze
    end
  end
end
