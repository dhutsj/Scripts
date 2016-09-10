# This script uploads test data into an elasticsearch index.
# it uses the EsFaker gem to generate the random metadata fields, 
# as well as multiple files of pregenerated strings

require_relative "EsFaker"
require 'rest-client'
require "json"
require 'optparse'
require 'securerandom'


# USE THIS IF YOU WANT THE SAME RANDOM NUMBERS EACH TIME!
srand 1234 

# Default values
host_array = Array.new
host_array = ["10.98.19.160","10.98.19.161","10.98.19.163"]
randomhost = rand(3)
$host = host_array[randomhost].to_s
#puts $host

$port = "445"
$cis_app_name = "test_app"
$index_name = "index01"

$app_index_list = Array.new

$dynamic_index = true
# Delete existing index or not
$delete_index = false
# Set to true for CIS
$cis = true
# Set the CIS version
$cis_ver = "v2.0"
# fci:  false metatdata, true fci
$fci = false
# thread_num: The concurrent request thread number
$thread_num = 1
# Number of bulk files to insert
#$num_bulk_files = 10
# Number of items in each bulk file
$items_in_bulk_file = 6
$update_in_bulk_file = 2
$delete_in_bulk_file = 1
$bulk_file_size = nil

$cis_username  = "administrator@CISPerf.COM" 
$cis_password  = "admpass1!"
$token = ""
# Size of FCI block - defaults to 100k, change as needed
$content_size = 102400
# Every x items is FCI - higher num is less FCI
$fci_count = 500 

#other global variable used 
$mutex_counter = Mutex.new
$bulk_url = ""

# Override RestClient to use timeout (900 seconds)
$timeout = 900
$open_timeout = 900

$execution_time = 10
$iteration = 0
$worker_thread_arr = []
$should_stop = false

$start_time = Time.now
$end_time = nil
$total_bulk_request_num = 0
$failed_bulk_request_num = 0
$total_doc_num = 0
$total_update_num = 0
$total_delete_num = 0
$failed_doc_num = 0

$ingested_doc_num = 0
$expect_ingest_doc_num = 10000000

$stats_interval = 60 # 60 minutes by default
$stats_collection = []

$error_file = nil
$result_file = nil
$ratio = 0
module RestClient2
  include RestClient

  def self.delete(url, headers={}, &block)
    Request.execute(:method => :delete, :url => url, :headers => headers, 
     :timeout => $timeout, :open_timeout => $open_timeout, &block)
  end

  def self.get(url, headers={}, &block)
    Request.execute(:method => :get, :url => url, :headers => headers, 
     :timeout => $timeout, :open_timeout => $open_timeout, &block)
  end

  def self.post(url, payload, headers={}, &block)
    Request.execute(:method => :post, :url => url, :payload => payload, :headers => headers,
     :timeout => $timeout, :open_timeout => $open_timeout, :content_type => :json, &block)
  end
end

class IngestStats
  attr_accessor :start_time
  attr_accessor :end_time
  attr_accessor :total_bulk_request_num
  attr_accessor :failed_bulk_request_num
  attr_accessor :total_doc_num
  attr_accessor :failed_doc_num
  attr_accessor :ingested_doc_num
  attr_accessor :total_update_num
  attr_accessor :total_delete_num
  def initialize(ingest_stats_collection = nil)
    
    @start_time = (ingest_stats_collection == nil) ? Time.now : Time.now + 1000000000
    @end_time = nil
    @total_bulk_request_num = 0
    @failed_bulk_request_num = 0
    @total_doc_num = 0
    @failed_doc_num = 0
    @ingested_doc_num = 0
    @total_update_num = 0
    @total_delete_num = 0
    if ingest_stats_collection != nil
      ingest_stats_collection.each do |ingest_stats|
        @start_time = ingest_stats.start_time if @start_time > ingest_stats.start_time
        @end_time = ingest_stats.end_time if (@end_time == nil || @end_time < ingest_stats.end_time)
        @total_bulk_request_num += ingest_stats.total_bulk_request_num
        @failed_bulk_request_num += ingest_stats.failed_bulk_request_num
        @total_doc_num += ingest_stats.total_doc_num
        @failed_doc_num += ingest_stats.failed_doc_num
        @ingested_doc_num +=ingest_stats.ingested_doc_num
        @total_update_num +=ingest_stats.total_update_num
        @total_delete_num +=ingest_stats.total_delete_num
      end
    end

  end

  def get_stats
    stats = ""
    stats << "Start time: " << @start_time.to_s << ".  "
    stats << "End time: " << @end_time.to_s << ".  "
    stats << "Total bulk requests: " << @total_bulk_request_num.to_s << ".  "
    stats << "Total docs: " << @total_doc_num.to_s << ".  "
    stats << "Failed bulk requests: " << @failed_bulk_request_num.to_s << ".  "
    stats << "Failed docs: " << @failed_doc_num.to_s << ".\n"
    stats << "Ingest rate (ingested docs per seconds): " << ((@ingested_doc_num - @failed_doc_num)/(@end_time - @start_time)).round.to_s << ".  "
    stats << "Total Ingested docs: " << @ingested_doc_num.to_s << ".  "
    stats << "Total Update docs: " << @total_update_num.to_s << ".  "
    stats << "Total Delete docs: " << @total_delete_num.to_s << ".  "
    return stats
  end

end


def get_doc_id(index_name)
    
    #doc_id = Array.new
    id_type = Hash.new
    cis_app_name = index_name.keys[0]
    real_index_name = index_name[cis_app_name]
    
    b_url = 'http://' + $host + ':9200' + '/' + cis_app_name + '~' + real_index_name + '~001' + '/_search?scroll=50m'
       #puts b_url
    json_query = '
          {
            "_source": false,
            "query": {
              "match_all": {}
            },
            "sort": [
              "_doc"
            ],
            
            "size": 3000
          }'
        #puts json_query
      arr = JSON(json_query)
      body = JSON.generate(arr)
        #puts body
          
    begin
       response  = RestClient::Request.execute(:method => :post, 
                                              :url => b_url,
                                              :payload => body,
                                              :content_type => :json,
                                              :accept => :json,
                                              :verify_ssl => false,
                                              :timeout => 1000000)

        responseMash = JSON.parse(response) 
        scroll_id = responseMash["_scroll_id"]
        #puts scroll_id
        hits = responseMash["hits"]
        hits = hits["hits"]
        hits.each do |item|
            parent_id = item["_id"]
            type = item["_type"]
            #puts parent_id
            #doc_id << parent_id
            id_type[parent_id] = type
            #puts doc_id      
        end
    end
    
    return id_type,scroll_id
end

def get_scroll_doc_id(scroll_id) 
        id_type = Hash.new
        begin
        scroll_url = 'http://' + $host + ':9200' + '/' + '_search/scroll/'
        hobj    = {"scroll" => "30m", "scroll_id" => "#{scroll_id}"}
        body    = hobj.to_json
        #puts body
        response  = RestClient::Request.execute(:method => :post, 
                                              :url => scroll_url,
                                              :payload => body,
                                              :content_type => :json,
                                              :accept => :json,
                                              :verify_ssl => false,
                                              :timeout => 1000000)

        responseMash = JSON.parse(response) 
        nextscroll_id = responseMash["_scroll_id"]
        #puts nextscroll_id
        hits = responseMash["hits"]
        hits = hits["hits"]
        hits.each do |item|
            parent_id = item["_id"]
            type = item["_type"]
            #puts parent_id
            #doc_id << parent_id
            id_type[parent_id] = type
            #puts doc_id      
        end
    end
    
    return id_type,nextscroll_id
end


# Get a block of random words for full content
def get_text()
  str_content = ""
  $content_size.times do |i|
    str_content = str_content + Faker::Metadata.get_file_name + " "
    break if str.length >= $content_size
  end
  return str_content
end

# Generate bulk file content
def generateBulkDocBody(insert_docCount,updateID_array,deleteID_array)
  bulk_plain = ""
  guid = SecureRandom.uuid

  (1..9*(insert_docCount/10)).each do |item_num|
    parent_id = guid + " " + item_num.to_s
    
    date_creation, date_modify = Faker::Metadata.get_dates
    isiowner, isiuid = Faker::Metadata.get_user
    isigroup, isigid = Faker::Metadata.get_group

    action_and_meta_data = Hash.new
    meta_data = Hash.new  

    #meta_data[:_index] = $index_name # Remove - all in same index

    meta_data[:_type] = "isilon_file"
    action_and_meta_data[:index] = meta_data
    meta_data[:_id] = parent_id
    action_and_meta_data[:index] = meta_data

    doc = Hash.new
    
    
    ext = Faker::Metadata.get_file_extension
    doc[:xvdate] = date_modify         
    doc[:xvtype] = ext
    doc[:xvplatform] = "Isilon"
    doc[:xvhaspreview] = false
    doc[:xvsize] = Faker::Metadata.get_size
    if $ratio % 1000 == 1 and item_num == 1
      doc[:xvname] = "Einstein.txt"
    elsif $ratio % 100 == 2 and item_num == 1
      doc[:xvname] = "ZincString.txt"
    elsif $ratio % 10 == 3 and item_num == 1
      doc[:xvname] = "EMCDell.txt"
    else
      doc[:xvname] = Faker::Metadata.get_file_name + "." + ext
    end
    if $ratio % 5000 == 1 and item_num == 1
      doc[:xvlocation] = Faker::Metadata.get_path_isilon + "/Einstein"
    elsif $ratio % 500 == 2 and item_num == 1
      doc[:xvlocation] = Faker::Metadata.get_path_isilon + "/ZincString"
    elsif $ratio % 50 == 3 and item_num == 1
      doc[:xvlocation] = Faker::Metadata.get_path_isilon + "/EMCDell"
    else
      doc[:xvlocation] = Faker::Metadata.get_path_isilon
    end
    
    doc[:isi_create_time] = date_creation
    doc[:isi_access_time] = date_modify
    doc[:isi_owner] = isiowner
    doc[:isi_group] = isigroup
    doc[:xvsource] = Faker::Metadata.get_server
    doc[:xvunindexable] = false
    
    doc[:isi_uid] = isiuid
    doc[:isi_gid] = isigid
    doc[:xviscontainer] = false
    doc[:xvparentid] = nil
    doc[:xvparentname] = nil
    doc[:isi_worm] = false
    doc[:isi_access_mode] = Faker::Metadata.get_isimode
    doc[:schemaversion] = nil
    doc[:xvhascontent] = false
    doc[:xvobjecttype] = "file"
    doc[:xvindexdate] = Time.now.strftime("%Y-%m-%dT%H:%M:%S.0Z")
	
    doc[:xvcontent] = nil
    doc[:xvpreview] = nil
    doc[:xvmetadata] = nil

    
    # Every x items, use full text
    #if item_num % $fci_count == 0
    if $fci == true
       doc[:hascontent] = true
       doc[:xvcontent] = full_text
    else
       doc[:hascontent] = false
       end


    bulk_plain << action_and_meta_data.to_json + "\n"
    bulk_plain << doc.to_json + "\n" 
  end
    
  (9*(insert_docCount/10)..(insert_docCount-1)).each do |item_num|
    parent_id = guid + " " + item_num.to_s
    
    date_creation, date_modify = Faker::Metadata.get_dates
    isiowner, isiuid = Faker::Metadata.get_user
    isigroup, isigid = Faker::Metadata.get_group

    action_and_meta_data = Hash.new
    meta_data = Hash.new  

    #meta_data[:_index] = $index_name # Remove - all in same index

    meta_data[:_type] = "isilon_folder"
    action_and_meta_data[:index] = meta_data
    meta_data[:_id] = parent_id
    action_and_meta_data[:index] = meta_data

    doc = Hash.new
    
    
    ext = Faker::Metadata.get_file_extension
    doc[:xvdate] = date_modify         
    doc[:xvtype] = ext
    doc[:xvplatform] = "Isilon"
    doc[:xvsize] = Faker::Metadata.get_size
    doc[:xvname] = Faker::Metadata.get_file_name 
    doc[:xvlocation] = Faker::Metadata.get_path_isilon
    doc[:isi_create_time] = date_creation
    doc[:isi_access_time] = date_modify
    doc[:isi_owner] = isiowner
    doc[:isi_group] = isigroup
    doc[:xvsource] = Faker::Metadata.get_server
    
    doc[:isi_uid] = isiuid
    doc[:isi_gid] = isigid
    doc[:xviscontainer] = false
    doc[:xvparentid] = nil
    doc[:xvparentname] = nil
    doc[:isi_worm] = false
    doc[:isi_access_mode] = Faker::Metadata.get_isimode
    doc[:schemaversion] = nil
    doc[:xvhascontent] = false
    doc[:xvobjecttype] = "file"
    doc[:xvindexdate] = Time.now.strftime("%Y-%m-%dT%H:%M:%S.0Z")
  
    bulk_plain << action_and_meta_data.to_json + "\n"
    bulk_plain << doc.to_json + "\n" 
    
    end  
    
    
  (insert_docCount..(insert_docCount+$update_in_bulk_file-1)).each do |item_num|
      
    parent_id = updateID_array[item_num-insert_docCount][0]
    #puts parent_id
    type = updateID_array[item_num-insert_docCount][1]
    #puts type
    if type == "isilon_file"
    action_and_meta_data = Hash.new
    meta_data = Hash.new  
    doc = Hash.new
    meta_data[:_type] = "isilon_file"
    action_and_meta_data[:update] = meta_data
    meta_data[:_id] = parent_id
    action_and_meta_data[:update] = meta_data
    
    ext = Faker::Metadata.get_file_extension
    doc[:xvname] = Faker::Metadata.get_file_name + "." + ext       
    doc[:xvlocation] = Faker::Metadata.get_path_isilon
    doc[:xvindexdate] = Time.now.strftime("%Y-%m-%dT%H:%M:%S.0Z")
    else 
    action_and_meta_data = Hash.new
    meta_data = Hash.new  
    doc = Hash.new
    meta_data[:_type] = "isilon_folder"
    action_and_meta_data[:update] = meta_data
    meta_data[:_id] = parent_id
    action_and_meta_data[:update] = meta_data
    
    
    doc[:xvname] = Faker::Metadata.get_file_name      
    doc[:xvlocation] = Faker::Metadata.get_path_isilon
    doc[:xvindexdate] = Time.now.strftime("%Y-%m-%dT%H:%M:%S.0Z") 
    end
    
    update_doc = Hash.new
    update_doc[:doc] = doc 
    bulk_plain << action_and_meta_data.to_json + "\n"
    bulk_plain << update_doc.to_json + "\n"
   #puts bulk_plain
     
    
   end
  
   
  ((insert_docCount+$update_in_bulk_file)..(insert_docCount+$update_in_bulk_file+$delete_in_bulk_file-1)).each do |item_num|
      
    parent_id = deleteID_array[item_num-insert_docCount-$update_in_bulk_file][0]
    type = deleteID_array[item_num-insert_docCount-$update_in_bulk_file][1]
    #puts parent_id
    
    if type == "isilon_file"
    action_and_meta_data = Hash.new
    meta_data = Hash.new  
    
    meta_data[:_type] = "isilon_file"
    action_and_meta_data[:delete] = meta_data
    meta_data[:_id] = parent_id
    action_and_meta_data[:delete] = meta_data
    else
    action_and_meta_data = Hash.new
    meta_data = Hash.new  
    
    meta_data[:_type] = "isilon_folder"
    action_and_meta_data[:delete] = meta_data
    meta_data[:_id] = parent_id
    action_and_meta_data[:delete] = meta_data
    end
    bulk_plain << action_and_meta_data.to_json + "\n"
    #puts bulk_plain
   
   
  end
   
    
 
  #puts bulk_plain
  return bulk_plain
end

def worker_thread_routine(index_name)
  
  #msg = "A working thread started: " + Thread.current.inspect
  #puts msg

  headers = {:'x-auth-token' => $token, :'Content-Type' => 'application/json'}

  retry_counter = 0
  scroll = Array.new
  docID_hash = Hash.new
  docID_hash, scroll_id = get_doc_id(index_name)
  docID_array = docID_hash.to_a
  scroll << scroll_id
  #puts scroll_id
  begin
    loop do 
      #puts $should_stop
      break if $should_stop

      #puts $app_index_list.length-1
      #cis_app_index = $app_index_list[$app_index_list.length-1]
      
      #puts cis_app_index
      cis_app_name = index_name.keys[0]
      #puts cis_app_name
      real_index_name = index_name[cis_app_name]
      #puts real_index_name

      if $cis   
        bulk_url = $base_url + 'app/' + cis_app_name + '/appindices/' + real_index_name + '/docs/_bulk?timeout=10000s'
        #puts bulk_url
      else
        bulk_url = $base_url + real_index_name + "/_bulk"
      end
      
      
          puts docID_array.length
      if (docID_array.length < ($update_in_bulk_file+$delete_in_bulk_file)) 
          #docID_hash.clear
          sleep 10 
          docID_hash, scroll_id = get_scroll_doc_id(scroll[-1]) 
          docID_array = docID_hash.to_a
          scroll << scroll_id
          #puts scroll
            if (docID_array.empty?)
              sleep 10 
              puts "30 minutes is too short, the scroll is has been expired!"
              docID_hash, scroll_id = get_doc_id(index_name)
              docID_array = docID_hash.to_a
              scroll << scroll_id
            end
              
      end
            
      
      
      
      updateID_array = docID_array.shift($update_in_bulk_file)
      
     
      deleteID_array = docID_array.shift($delete_in_bulk_file)
      
      bulk_data = generateBulkDocBody($items_in_bulk_file,updateID_array,deleteID_array)
      
      $ratio +=1
      $bulk_file_size = bulk_data.to_s.size/1024 if $bulk_file_size == nil

      response = nil
        
      begin
      
        response  = RestClient::Request.execute(:method => :post, 
                                              :url => bulk_url,
                                              :payload => bulk_data,
                                              :content_type => :json,
                                              :accept => :json,
                                              :verify_ssl => false,
                                              :headers => headers,
                                              :timeout => 1000000,
                                              )
        
        #puts "#{Thread.current.to_s}. Executed a bulk request. "
        sleep 10
        json_resp = JSON.parse response
        #puts json_resp
        errors = json_resp["errors"]
        #puts errors
        
        if errors # some items were failed
          failed = 0
          json_resp["items"].each do |item|
            if item["status"] != 200 && item["status"] != 201
              failed += 1
              # write the response to the error file
              $error_file.write(item.to_s + "\n")
            end
          end

          $error_file.flush

          $mutex_counter.synchronize do
            $total_bulk_request_num += 1
            $failed_bulk_request_num += 1
            $total_doc_num += $items_in_bulk_file + $update_in_bulk_file + $delete_in_bulk_file
            $failed_doc_num += failed
            #puts $failed_doc_num
            
            $ingested_doc_num = $ingested_doc_num + $items_in_bulk_file - $failed_doc_num
            #puts $ingested_doc_num
            $total_update_num += $update_in_bulk_file - $failed_doc_num 
            $total_delete_num += $delete_in_bulk_file - $failed_doc_num
          end
        else # all items were succeeful
          $mutex_counter.synchronize do
            $total_bulk_request_num += 1
            $total_doc_num += $items_in_bulk_file + $update_in_bulk_file + $delete_in_bulk_file
            $ingested_doc_num += $items_in_bulk_file
            #puts $ingested_doc_num
            $total_update_num += $update_in_bulk_file
            $total_delete_num += $delete_in_bulk_file
            
          end
        end # if errors
        
      rescue => e
        msg = "#{Time.now.to_s}. #{Thread.current.inspect}. Fatal ERROR while inserting bulk file. Error:  #{e.message}.\n}"
        msg << "URL: #{bulk_url}. Response: #{response.inspect}."
        puts msg
        $error_file.write(msg + "\n")
        $error_file.flush

        #puts "Saving bulk file to errorbulk.json..."

        $mutex_counter.synchronize do
          $total_bulk_request_num += 1
          $failed_bulk_request_num += 1
          $total_doc_num += $items_in_bulk_file + $update_in_bulk_file + $delete_in_bulk_file
          $failed_doc_num += $items_in_bulk_file
        end

        retry_counter += 1

        # we will retry 3 times, then quit the thread
        break if retry_counter >= 3

        # sleep 10 seconds for each retry
        sleep 10
      end
    end # end loop

  rescue => e
    msg = "#{Time.now.to_s}. #{Thread.current.inspect}. Fatal Error: " + e.message
    puts msg
    $error_file.write(msg + "\n")
    $error_file.flush
  end
      
  #puts "A worker thread stopped: " + Thread.current.inspect
end

def monitor_thread_routine(stop_time)

  # check if we should stop
  loop do
      
      if Time.now >= stop_time || ($ingested_doc_num >= $expect_ingest_doc_num)
      puts "It's the end time, we are going to quit."
      break
    end
       

    # check if we should collect performance statistics
    if (Time.now - $start_time) >= $stats_interval

      current_stats = nil
      current_stats = IngestStats.new()

      $mutex_counter.synchronize do
        current_stats.start_time = $start_time
        current_stats.end_time = Time.now
        current_stats.total_bulk_request_num = $total_bulk_request_num
        current_stats.failed_bulk_request_num = $failed_bulk_request_num
        current_stats.total_doc_num = $total_doc_num
        current_stats.ingested_doc_num = $ingested_doc_num
        current_stats.total_update_num = $total_update_num
        current_stats.total_delete_num = $total_delete_num    
        current_stats.failed_doc_num = $failed_doc_num

        # reset
        $start_time = Time.now
        $end_time = nil
        $total_bulk_request_num = 0
        $failed_bulk_request_num = 0
        $total_doc_num = 0
        $failed_doc_num = 0
        $ingested_doc_num = 0
        $total_update_num = 0
        $total_delete_num = 0
      end

      $stats_collection << current_stats

      msg = "\nIngest Statistics during past time\n"
      msg << current_stats.get_stats
      
      puts msg
      $result_file.write(msg + "\n")

      # flush the buffer
      $result_file.flush

    end

    sleep 10

    # check if there is alive worker thread
    worker_threads_alive = false

    # check if all worker threads stopped
    $worker_thread_arr.each do |t|
      if t.alive?
        worker_threads_alive = true
        break
      end
    end

    # quit if there is no worked thread alive
    break if !worker_threads_alive

  end

  puts "Notity the worker threads to stop. Wait..."

  # notify worker threads to stop
  $should_stop = true

  # wait worker threads to stop.
  # we will wait at most 300 seconds
  waiting_time = 300

  while (waiting_time > 0) do
    all_stopped = true

    # check if all worker threads stopped
    $worker_thread_arr.each do |t|
      if t.alive?
        all_stopped = false
        break
      end
    end

    if all_stopped
      break
    else
      sleep 10
      waiting_time -= 10
    end

  end #end while

  # kill worker threads if they are alive
  $worker_thread_arr.each do |t|
    if t.alive?
      t.kill
      puts "Worker thread #{t.inspect} is been killed"
    end
  end

  # get last performace stats
  current_stats = IngestStats.new()
  current_stats.start_time = $start_time
  current_stats.end_time = Time.now
  current_stats.total_bulk_request_num = $total_bulk_request_num
  current_stats.failed_bulk_request_num = $failed_bulk_request_num
  current_stats.total_doc_num = $total_doc_num
  current_stats.ingested_doc_num = $ingested_doc_num
  current_stats.total_update_num = $total_update_num
  current_stats.total_delete_num = $total_delete_num
  current_stats.failed_doc_num = $failed_doc_num

  $stats_collection << current_stats

  # output last statistics
  msg = "\nIngest Statistics during past time\n"
  msg << current_stats.get_stats
  
  puts msg
  $result_file.write(msg + "\n")

  # flush the buffer
  $result_file.flush

end

def get_application_indices()

    #puts "Getting all applications."
    headers   = {:'x-auth-token' => $token}
    surl    = $base_url + 'app/_all_applications'
        
          begin
            response  = RestClient::Request.execute(:method => :get, :url => surl, :content_type => :json, :accept => :json, :verify_ssl => false, :headers => headers)
          rescue => e
            puts "Cannot get the applications"
            return
          end

    responseMash = JSON.parse(response.body)
    apps = Array.new
    apps = responseMash.keys
    
    index_names = Array.new
    
    apps.each do |app_name|
      #puts app_name.to_s
      surl    = $base_url + 'app/' + app_name + '/appindices/_all'
      begin
          response  = RestClient::Request.execute(:method => :get, :url => surl, :content_type => :json, :accept => :json, :verify_ssl => false, :headers => headers)
          rescue => e
            puts "Cannot get the indices"
            return
       end
       responseMash = JSON.parse(response.body)
       index_names = responseMash.keys
       index_names.each do |index_name|
          app_index = Hash.new
          app_index[app_name] = index_name
          $app_index_list << app_index    
       end
    end 
end



# Create application and index
def prepare_index()

  if $cis
    #puts $host
    $base_url = 'https://' + $host + ':' + $port + '/cis/' + $cis_ver + '/'
    #puts $host
  else
    $base_url = 'http://' + $host + ':' + $port + '/'
  end
  
  thehost = $host + ":" + $port
  
  puts "--------------------------------"
  puts "Host: " + thehost
  puts
  
  array_hash = []
  
  # If CIS, authenticate and create application (if it doesn't exist)
  if $cis
    puts "Authenticating: " + $cis_username + " (" + $cis_password + ")"
    surl      = $base_url + 'auth/authenticate'
    hobj    = {'username' => $cis_username, 'password' => $cis_password}
    body    = hobj.to_json
    begin
      response  = RestClient::Request.execute(:method => :post, :url => surl, :payload => body, :content_type => :json, :accept => :json, :verify_ssl => OpenSSL::SSL::VERIFY_NONE)
      $token = response.headers[:http_x_auth_token]
    rescue => e
      puts "Failed to authenticate: " + e.message
      return
    end
  
    create_app = false
    # Verifying Application
    puts "Getting application: #{$cis_app_name}"
    headers   = {:'x-auth-token' => $token}
    surl    = $base_url + 'app/' + $cis_app_name
  
    begin
      response  = RestClient::Request.execute(:method => :get, :url => surl, :content_type => :json, :accept => :json, :verify_ssl => false, :headers => headers)
    rescue => e
      puts "Application not found, creating"
      create_app = true
    end
  
    if create_app
      body    = ""
      surl    = $base_url + 'app/' + $cis_app_name
      hobj    = {"option1"=>"basic", "option2"=>"true,"}
      body    = JSON.generate(hobj)
      begin
        # different API for v1.1 vs 2.0?
        if $cis_ver == "v1.1"
          response  = RestClient::Request.execute(:method => :put, :url => surl, :payload => body, :content_type => :json, :accept => :json, :verify_ssl => false, :headers => headers)
        else
          response  = RestClient::Request.execute(:method => :post, :url => surl, :payload => body, :content_type => :json, :accept => :json, :verify_ssl => false, :headers => headers)
        end
      rescue Exception => e
        puts "Failed to create app: " + e.message
        exit
      end
    end
  end
  
  # index url
  if ($cis)
      surl = $base_url + 'app/' + $cis_app_name + '/appindices/' + $index_name
    else
      surl = $base_url + $index_name
  end

  index_exists = false
  # check if index exists
  begin
    response  = RestClient::Request.execute(:method => :get, :url => surl, :content_type => :json, :accept => :json, :verify_ssl => false, :headers => headers)
    index_exists = true
  rescue => e
    puts "Index not found, creating"
  end
  
  # Delete index if required
  if $delete_index && index_exists
    puts "Deleting index: #{$index_name}"

    begin
      response  = RestClient::Request.execute(:method => :delete, :url => surl, :content_type => :json, :accept => :json, :verify_ssl => false, :headers => headers)
    rescue Exception => e
      puts "Failed to delete index, may not exist - " + e.message
    end

    index_exists = false
  end
  
  # Create an index
  if !index_exists
    puts "Creating index: #{$index_name}"
      
    # Read mappings
    file_json = open("index.json")
    string_json = file_json.read
    file_json.close
    #puts surl
    #puts string_json
    if $dynamic_index
      surl = surl + '?index_type=expand_only'
    end

    begin
      # different API for v1.1 vs 2.0?
      if $cis_ver == "v1.1"
        response  = RestClient::Request.execute(:method => :put, :url => surl, :payload => string_json, :content_type => :json, :accept => :json, :verify_ssl => false, :headers => headers)
      else
        response  = RestClient::Request.execute(:method => :post, :url => surl, :payload => string_json, :content_type => :json, :accept => :json, :verify_ssl => false, :headers => headers)
      end
    
      puts "Index created"
    rescue Exception => e
      puts "Failed to create index, may already exist - " + e.message
    end

  end # if !index_exists
    
end



def run_prepare()
  
  # Read optional values
  options = {}
  OptionParser.new do |opts|
    opts.banner = "Usage: Insert_threading.rb [options]"
  
    opts.on('-h', '--host HOST', 'CIS or Elasticsearch host') { |v| $host = v }
    opts.on('-p', '--port PORT', 'CIS or Elasticsearch port') { |v| $port = v }
    opts.on('-d', '--delete', 'Delete existing index') { $delete_index = true }
    opts.on('--elasticsearch', 'Use Elasticsearch') { $cis = false }
    opts.on('-k', '--fci', 'fci:  false metatdata, true fci') { $fci = true }
    opts.on('-v', '--cis_ver VER', 'Specific CIS version (defaults to "v2.0")') { |v| $cis_ver = v }
    opts.on('-u', '--cis_user USER', 'CIS Username (defaults to adminsitrator@domain.com)') { |v| $cis_username = v }
    opts.on('-w', '--cis_pw PASSWORD', 'CIS Password (defaults to qampass1!)') { |v| $cis_password = v }
    opts.on('-f', '--fci_size SIZE', 'Size of FCI block in bytes (defaults to 100k)') { |v| $content_size = v.to_i }
    opts.on('-b', '--bulk_items NUM', 'Number of items in each bulk file (defaults to 100)') { |v| $items_in_bulk_file = v.to_i }
    opts.on('-u', '--update_bulk_items NUM', 'Number of update items in each bulk file (defaults to 100)') { |v| $update_in_bulk_file = v.to_i }
    opts.on('-f', '--delete_bulk_items NUM', 'Number of delete items in each bulk file (defaults to 100)') { |v| $delete_in_bulk_file = v.to_i }
    opts.on('-a', '--app_name NAME', 'Name of CIS application (defaults to test_app)') { |v| $cis_app_name = v }
    opts.on('-i', '--index_name NAME', 'Name of Index (defaults to ingest)') { |v| $index_name = v }
    # Multiple thread support
    opts.on('-t', '--thread_number NUMBER', 'The concurrent request thread number (default to 4) ') { |v| $thread_num = v.to_i; }
    opts.on('-s', '--execution time', 'The execution time in minutes (default to 10 minutes) ') { |v| $execution_time = v.to_i; }
    opts.on('-e', '--performance_interval time', 'The interval in minutes to collect performance data (default to 60 minutes) ') { |v| $stats_interval = v.to_i * 60; }
    opts.on('-c', '--document count', 'Document count to be ingested (default is 1 billion ) ') { |v| $expect_ingest_doc_num = v.to_i; }
    
  end.parse!
  
  prepare_index()
  
  #filename = "Result_"+ Time.now.strftime("%Y-%m-%d-%H-%M-%S")+".txt"
  filename = "ingest_log_#{$items_in_bulk_file}_#{$thread_num}.txt"
  $result_file = File.new(filename, "w")

  $result_file.write("Start at #{Time.now.to_s}\n\n")

  $error_file = File.open("errorbulk.json", 'w') 
  
  # puts"------------------------------------------------------"
  # msg =  "Number of bulk files " + $num_bulk_files.to_s
  # puts msg
  # $fr.write(msg + "\n")
 
  msg =  "Items per bulk file: " + $items_in_bulk_file.to_s
  puts msg
  $result_file.write(msg + "\n")
  
  msg =  "Thread number: " + $thread_num.to_s
  puts msg
  $result_file.write(msg + "\n")
  puts"------------------------------------------------------" 
   
end

def get_document_count_in_index()
  doc_count = 0
  index_name = $index_name
  url = "http://" + $host
  if $cis
    index_name = $cis_app_name + "~index*"
    url += ":9200" 
  else
    url += $port
  end

  url += "/" + index_name + "/_count"

  begin
    response  = RestClient::Request.execute(:method => :get, 
                                              :url => url,
                                              :content_type => :json,
                                              :accept => :json,
                                              :verify_ssl => false,
                                              :timeout => 1000)

    response = JSON.parse(response)
    doc_count = response["count"]
  rescue Exception => e
    puts "Failed to get document count in index - " + e.message
  end

  return doc_count
end

def run()
 
  $start_time = Time.now

  # spawn the monitor thread
  monitor_thread = Thread.new{ monitor_thread_routine(Time.now + $execution_time * 60) }

  # spawn multiple worker threads
  #$worker_thread_arr << Thread.new{ worker_thread_routine() }

  #for i in 0..$app_index_list.length-1
  (0..$app_index_list.length-1).each do |i|
    
    #puts $app_index_list[i]
    
    $worker_thread_arr << Thread.new{ worker_thread_routine($app_index_list[i]) }
    #$worker_thread_arr[i].join()
    #monitor_thread.join()
  end
    #$worker_thread_arr.each do |t|
           #t.join()
    #end

  # wait monitor thread to complete
  monitor_thread.join()

end

if __FILE__ == $0
  
  run_prepare()

  get_application_indices()

  initial_doc_num = get_document_count_in_index

  run()

  final_doc_num = get_document_count_in_index

  $result_file.write("\nEnd at #{Time.now.to_s}\n\n")

  msg = "\nTotal document number in index BEFORE ingest: " +  initial_doc_num.to_s + "\n"
  msg << "Total document number in index AFTER ingest: " +  final_doc_num.to_s + "\n"
  msg << "New added document number: " +  (final_doc_num - initial_doc_num).to_s + "\n"

  msg << "\nAverage body size for one Bulk file: " + $bulk_file_size.to_s + " KB"
  puts msg
  $result_file.write(msg + "\n")
  
  summary_stats = IngestStats.new($stats_collection)
  msg = "\nIngest Summary: \n"
  msg << summary_stats.get_stats
  
  puts msg
  $result_file.write(msg + "\n")

  $result_file.close
  $error_file.close
end


