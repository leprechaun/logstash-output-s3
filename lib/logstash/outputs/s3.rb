# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/aws_config"
require "stud/temporary"
require "socket" # for Socket.gethostname
require "thread"
require "tmpdir"
require "fileutils"


# INFORMATION:
#
# This plugin was created for store the logstash's events into Amazon Simple Storage Service (Amazon S3).
# For use it you needs authentications and an s3 bucket.
# Be careful to have the permission to write file on S3's bucket and run logstash with super user for establish connection.
#
# S3 plugin allows you to do something complex, let's explain:)
#
# S3 outputs create temporary files into "/opt/logstash/S3_temp/". If you want, you can change the path at the start of register method.
# This files have a special name, for example:
#
# ls.s3.ip-10-228-27-95.2013-04-18T10.00.tag_hello.part0.txt
#
# ls.s3 : indicate logstash plugin s3
#
# "ip-10-228-27-95" : indicate you ip machine, if you have more logstash and writing on the same bucket for example.
# "2013-04-18T10.00" : represents the time whenever you specify time_file.
# "tag_hello" : this indicate the event's tag, you can collect events with the same tag.
# "part0" : this means if you indicate size_file then it will generate more parts if you file.size > size_file.
#           When a file is full it will pushed on bucket and will be deleted in temporary directory.
#           If a file is empty is not pushed, but deleted.
#
# This plugin have a system to restore the previous temporary files if something crash.
#
##[Note] :
#
## If you specify size_file and time_file then it will create file for each tag (if specified), when time_file or
## their size > size_file, it will be triggered then they will be pushed on s3's bucket and will delete from local disk.
## If you don't specify size_file, but time_file then it will create only one file for each tag (if specified).
## When time_file it will be triggered then the files will be pushed on s3's bucket and delete from local disk.
#
## If you don't specify time_file, but size_file  then it will create files for each tag (if specified),
## that will be triggered when their size > size_file, then they will be pushed on s3's bucket and will delete from local disk.
#
## If you don't specific size_file and time_file you have a curios mode. It will create only one file for each tag (if specified).
## Then the file will be rest on temporary directory and don't will be pushed on bucket until we will restart logstash.
#
#
# #### Usage:
# This is an example of logstash config:
# [source,ruby]
# output {
#    s3{
#      access_key_id => "crazy_key"             (required)
#      secret_access_key => "monkey_access_key" (required)
#      endpoint_region => "eu-west-1"           (required)
#      bucket => "boss_please_open_your_bucket" (required)
#      size_file => 2048                        (optional)
#      time_file => 5                           (optional)
#      format => "plain"                        (optional)
#      canned_acl => "private"                  (optional. Options are "private", "public_read", "public_read_write", "authenticated_read". Defaults to "private" )
#    }
#
class LogStash::Outputs::S3 < LogStash::Outputs::Base
  include LogStash::PluginMixins::AwsConfig

  TEMPFILE_EXTENSION = "txt"
  S3_INVALID_CHARACTERS = /[\^`><]/

  config_name "s3"
  default :codec, 'line'

  # S3 bucket
  config :bucket, :validate => :string

  # AWS endpoint_region
  config :endpoint_region, :validate => ["us-east-1", "us-west-1", "us-west-2",
                                         "eu-west-1", "ap-southeast-1", "ap-southeast-2",
                                        "ap-northeast-1", "sa-east-1", "us-gov-west-1"], :deprecated => 'Deprecated, use region instead.'

  # Set the size of file in bytes, this means that files on bucket when have dimension > file_size, they are stored in two or more file.
  # If you have tags then it will generate a specific size file for every tags
  ##NOTE: define size of file is the better thing, because generate a local temporary file on disk and then put it in bucket.
  config :size_file, :validate => :number, :default => 0

  # Set the time, in minutes, to close the current sub_time_section of bucket.
  # If you define file_size you have a number of files in consideration of the section and the current tag.
  # 0 stay all time on listerner, beware if you specific 0 and size_file 0, because you will not put the file on bucket,
  # for now the only thing this plugin can do is to put the file when logstash restart.
  config :time_file, :validate => :number, :default => 0

  ## IMPORTANT: if you use multiple instance of s3, you should specify on one of them the "restore=> true" and on the others "restore => false".
  ## This is hack for not destroy the new files after restoring the initial files.
  ## If you do not specify "restore => true" when logstash crashes or is restarted, the files are not sent into the bucket,
  ## for example if you have single Instance.
  config :restore, :validate => :boolean, :default => false

  # The S3 canned ACL to use when putting the file. Defaults to "private".
  config :canned_acl, :validate => ["private", "public_read", "public_read_write", "authenticated_read"],
         :default => "private"

  # Set the directory where logstash will store the tmp files before sending it to S3
  # default to the current OS temporary directory in linux /tmp/logstash
  config :temporary_directory, :validate => :string, :default => File.join(Dir.tmpdir, "logstash")

  # Specify a prefix to the uploaded filename, this can simulate directories on S3
  config :prefix, :validate => :string, :default => ''

  # Specify how many workers to use to upload the files to S3
  config :upload_workers_count, :validate => :number, :default => 1

  # Exposed attributes for testing purpose.
  attr_accessor :tempfile
  attr_reader :page_counter
  attr_reader :s3

  def aws_s3_config
    @logger.info("Registering s3 output", :bucket => @bucket, :endpoint_region => @region)
    @s3 = AWS::S3.new(aws_options_hash)
  end

  def aws_service_endpoint(region)
    # Make the deprecated endpoint_region work
    # TODO: (ph) Remove this after deprecation.
    
    if @endpoint_region
      region_to_use = @endpoint_region
    else
      region_to_use = @region
    end

    return {
      :s3_endpoint => region_to_use == 'us-east-1' ? 's3.amazonaws.com' : "s3-#{region_to_use}.amazonaws.com"
    }
  end

  public
  def write_on_bucket(file)
    # find and use the bucket
    bucket = @s3.buckets[@bucket]

    remote_filename = file.gsub(@temporary_directory, "").sub!(/^\//, '')

		split = remote_filename.split(".")

		split.pop

		split << ""

		@logger.info("write_on_bucket: #{remote_filename}")

    File.open(file, 'r') do |fileIO|
      begin
        # prepare for write the file
        object = bucket.objects[remote_filename]
        object.write(fileIO, :acl => @canned_acl)
      rescue AWS::Errors::Base => error
        @logger.error("S3: AWS error", :error => error)
        raise LogStash::Error, "AWS Configuration Error, #{error}"
      end
    end

    @logger.debug("S3: has written remote file in bucket with canned ACL", :remote_filename => remote_filename, :bucket  => @bucket, :canned_acl => @canned_acl)
  end

  public
  def register
    require "aws-sdk"
    # required if using ruby version < 2.0
    # http://ruby.awsblog.com/post/Tx16QY1CI5GVBFT/Threading-with-the-AWS-SDK-for-Ruby
    AWS.eager_autoload!(AWS::S3)

    workers_not_supported

    @s3 = aws_s3_config
    @upload_queue = Queue.new
    @file_rotation_lock = Mutex.new

    if @prefix && @prefix =~ S3_INVALID_CHARACTERS
      @logger.error("S3: prefix contains invalid characters", :prefix => @prefix, :contains => S3_INVALID_CHARACTERS)
      raise LogStash::ConfigurationError, "S3: prefix contains invalid characters"
    end

    if !Dir.exist?(@temporary_directory)
      FileUtils.mkdir_p(@temporary_directory)
    end

		@segregations = {}

    test_s3_write

    restore_from_crashes if @restore == true
		register_segregation("test/test")
    configure_periodic_rotation if time_file != 0
		@segregations.delete("test/test")
  #  configure_upload_workers

    @codec.on_event do |event, encoded_event|
      handle_event(event, encoded_event)
    end
  end


  # Use the same method that Amazon use to check
  # permission on the user bucket by creating a small file
  public
  def test_s3_write
    @logger.debug("S3: Creating a test file on S3")

    test_filename = File.join(
			@temporary_directory,
			"logstash-programmatic-access-test-object-#{Time.now.to_i}"
		)

    File.open(test_filename, 'a') do |file|
      file.write('test')
    end

    begin
      write_on_bucket(test_filename)
      delete_on_bucket(test_filename)
    ensure
      File.delete(test_filename)
    end
  end

  public
  def restore_from_crashes
    @logger.debug("S3: is attempting to verify previous crashes...")

    Dir[File.join(@temporary_directory, "*.#{TEMPFILE_EXTENSION}")].each do |file|
      name_file = File.basename(file)
      @logger.warn("S3: have found temporary file the upload process crashed, uploading file to S3.", :filename => name_file)
      move_file_to_bucket_async(file)
    end
  end

  public
  def periodic_interval
    @time_file * 60
  end

  public
  def get_temporary_filename(directory, file, page_counter = 0)
		# Just to make sure we don't over-write files from a 'concurrent' logstash instance
		# this includes a node that was replaced and gets it's part number reset
		rand_string = (0...8).map { (65 + rand(26)).chr }.join
    return "#{@temporary_directory}/#{directory}/#{file}.part-#{page_counter}.#{rand_string}.#{TEMPFILE_EXTENSION}"
  end

  public
  def receive(event)
    return unless output?(event)
    @codec.encode(event)
  end

  public
  def rotate_events_log? segregation
    @file_rotation_lock.synchronize do
      @segregations[segregation][:file].size > @size_file
    end
  end

  public
  def write_events_to_multiple_files?
    @size_file > 0
  end

  public
  def teardown
    shutdown_upload_workers
    @periodic_rotation_thread.stop! if @periodic_rotation_thread

    @file_rotation_lock.synchronize do
      @tempfile.close unless @tempfile.nil? && @tempfile.closed?
    end
    finished
  end

  private
  def shutdown_upload_workers
    @logger.debug("S3: Gracefully shutdown the upload workers")
    @upload_queue << LogStash::ShutdownEvent
  end

	private
	def extract_base(segregation)
		dirs = segregation.split("/")
		dir = dirs[0..(dirs.length- 2)]
		file = dirs[-1]
		return [dir.join("/"), file]
	end

	private
	def register_segregation segregation
		# Register the aggregation (file pointer, page counter and timestamp)
		unless @segregations.keys.include? segregation
			@logger.info("register new segregation: #{segregation}")

			directory, file = extract_base(segregation)
			@logger.info(:directory => directory, :file => file)

			begin
				temp_dir = @temporary_directory + "/" + directory
				FileUtils.mkdir_p(temp_dir)
				@logger.info("created directory: #{directory}")
			
				@segregations[segregation] = {
					:start_time => Time.now.to_i,
					:directory => directory,
					:file_base => file,
					:current_page => 0,
					:file_pointers => {
						0 => File.open(get_temporary_filename(directory, file, 0), 'a'),
					}
				}
			rescue StandardError => e
				@logger.info(e)
				@logger.info("Failed to create temp directory")
				raise e
			end
		end
	end

	def commit_locally(segregation, event, encoded_event)
		seg = @segregations[segregation]
		if seg[:file_pointers][seg[:current_page]].syswrite(encoded_event)
			@logger.info("S3> commit_locally: write success (file: #{seg[:file_pointers][seg[:current_page]].path}")
		else
			# What do?
			@logger.info("S3> commit_locally: write fail (file: #{seg[:file_pointers][seg[:current_page]].path}")
		end

	end

	# Only based on file_size, time is in another thread
	def should_commit?(segregation)
		seg = @segregations[segregation]
		if @size_file > 0 and seg[:file_pointers][seg[:current_page]].size > @size_file
			@logger.info("S3> should_commit: upload because of size")
			return true
		end

		return false
	end

	def commit(segregation)
		current_page = @segregations[segregation][:current_page]
		time_start = @segregations[segregation][:start_time]

		next_page(segregation)

		Stud::Task.new do
			LogStash::Util::set_thread_name("S3> thread: commit")
			upload_and_delete(segregation, current_page, time_start)
		end

	end

	def upload_and_delete(segregation, page_to_upload, time_start)
		begin
			@logger.info("in thread")
			seg = @segregations[segregation]
			bucket = @s3.buckets[@bucket]
			key = seg[:file_pointers][page_to_upload].path.gsub(@temporary_directory, "").sub!(/^\//, '')
			@logger.info("write_on_bucket: #{key}")
			@file_rotation_lock.synchronize do
				@logger.info("#{segregation} size is #{seg[:file_pointers][page_to_upload].size}")
				if seg[:file_pointers][page_to_upload].size > 0
					File.open(seg[:file_pointers][page_to_upload].path, 'r') do |fileIO|
						begin
							# prepare for write the file
							object = bucket.objects[key]
							object.write(fileIO, :acl => @canned_acl)
							@logger.debug("S3: has written remote file in bucket with canned ACL", :remote_filename => key, :bucket  => @bucket, :canned_acl => @canned_acl)
						rescue AWS::Errors::Base => error
							@logger.error("S3: AWS error", :error => error)
							raise LogStash::Error, "AWS Configuration Error, #{error}"
						end
					end
				else
					@logger.info("don't upload: size <= 0")
				end

				FileUtils.rm(@segregations[segregation][:file_pointers][page_to_upload].path)
				@segregations[segregation][:file_pointers].delete(page_to_upload)
			end

		rescue StandardError => e
			@logger.info(e)
			raise e
		end

	end

	private
	def handle_event(event, encoded_event)
		segregation = event.sprintf(@prefix)

		register_segregation(segregation)

		commit_locally(segregation, event, encoded_event)

		if should_commit? segregation
			commit(segregation)
		end
	end

	private
	def configure_periodic_rotation
		@periodic_rotation_thread = Stud::Task.new do
			begin
				LogStash::Util::set_thread_name("S3> thread: periodic_uploader")
				begin
					Stud.interval(periodic_interval, :sleep_then_run => true) do
						begin
							@logger.info("running periodic uploader ... but may not see new segregations")
							@segregations.each { |segregation, values|
								commit(segregation)
							}
						rescue StandardError => e
							@logger.info(e)
						end
					end
				rescue StandardError => e
					@logger.info(e)
				end
			rescue StandardError => e
				@logger.info(e)
			end
		end
	end

	private
	def next_page( segregation )
		seg = @segregations[segregation]
		seg[:current_page] = seg[:current_page] + 1
		seg[:file_pointers][seg[:current_page]] = File.open(
			get_temporary_filename(
				@segregations[segregation][:directory],
				@segregations[segregation][:file_base],
				@segregations[segregation][:current_page]
			),
		'a')
	end


  private
  def delete_on_bucket(filename)
    bucket = @s3.buckets[@bucket]

    remote_filename = "#{@prefix}#{File.basename(filename)}"

    @logger.debug("S3: delete file from bucket", :remote_filename => remote_filename, :bucket => @bucket)

    begin
      # prepare for write the file
      object = bucket.objects[remote_filename]
      object.delete
    rescue AWS::Errors::Base => e
      @logger.error("S3: AWS error", :error => e)
      raise LogStash::ConfigurationError, "AWS Configuration Error"
    end
  end
end
