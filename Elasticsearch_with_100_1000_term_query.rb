require_relative "EsFakerGetPath"
require 'rest-client'
require "json"
require 'optparse'
require 'securerandom'
require 'hashie'

class CISQuery

    attr_accessor :host
    attr_accessor :port
    attr_accessor :cis_ver
    attr_accessor :cis_app_name
    attr_accessor :index_name

    attr_accessor :cis_username
    attr_accessor :cis_password
    attr_accessor :token

    def initialize()
        @host = "10.98.19.160"
        @port = "445"
        @cis_ver = "v2.0"
        @cis_app_name = "test_app"
        @index_name = "index01"

        @cis_username  = "administrator@CISPerf.COM" 
        @cis_password  = "admpass1!"
        @token = ""   
    end

    def admin_login()
        
          @base_url = 'https://' + @host + ':' + @port + '/cis/' + @cis_ver + '/'

          puts "Authenticating: " + @cis_username + " (" + @cis_password + ")"
          surl      = @base_url + 'auth/authenticate'
          hobj    = {'username' => @cis_username, 'password' => @cis_password}
          body    = hobj.to_json
          begin
            response  = RestClient::Request.execute(:method => :post, :url => surl, :payload => body, :content_type => :json, :accept => :json, :verify_ssl => OpenSSL::SSL::VERIFY_NONE)
            @token = response.headers[:http_x_auth_token]
          rescue => e
            puts "Failed to authenticate: " + e.message
            return
          end
        
    end

    def verify_application()

      # Verifying Application
      puts "Verifying application: #{@cis_app_name}"
      headers   = {:'x-auth-token' => @token}
      surl    = @base_url + 'app/' + @cis_app_name
    
      begin
        response  = RestClient::Request.execute(:method => :get, :url => surl, :content_type => :json, :accept => :json, :verify_ssl => false, :headers => headers)
      rescue => e
      puts "Application not found, exiting"
      exit
      end
      
      @surl = @base_url + 'app/' + @cis_app_name + '/appindices/' + @index_name + '/docs/_search'
      #@surl = @base_url + 'app/' + @cis_app_name + '/appindices/docs/_search'
      
    end

    def prepare_index()

 
        @base_url = 'https://' + @host + ':' + @port + '/cis/' + @cis_ver + '/'
        
        # Login with administrator
       
        admin_login()
        
        # Create application if it doesn't exist
        create_app = false
        # Verifying Application
        puts "Getting application: #{@cis_app_name}"
        headers   = {:'x-auth-token' => @token}
        surl    = @base_url + 'app/' + @cis_app_name
      
        begin
          response  = RestClient::Request.execute(:method => :get, :url => surl, :content_type => :json, :accept => :json, :verify_ssl => false, :headers => headers)
        rescue => e
          puts "Application not found, creating"
          create_app = true
        end
      
        if create_app
          body    = ""
          surl    = @base_url + 'app/' + @cis_app_name
          hobj    = {"option1"=>"basic", "option2"=>"true,"}
          body    = JSON.generate(hobj)
          begin
              response  = RestClient::Request.execute(:method => :post, :url => surl, :payload => body, :content_type => :json, :accept => :json, :verify_ssl => false, :headers => headers)
          rescue Exception => e
            puts "Failed to create app: " + e.message
            exit
          end
        end
     
        
         
          
          # Delete index if required
          if @delete_index
            puts "Deleting index: #{@index_name}"
              
            surl      = @base_url + 'app/' + @cis_app_name + '/appindices/' + @index_name
            headers   = {:'x-auth-token' => @token}
            body    = ""
            begin
              response  = RestClient::Request.execute(:method => :delete, :url => surl, :payload => body, :content_type => :json, :accept => :json, :verify_ssl => false, :headers => headers)
            rescue Exception => e
              puts "Failed to delete index, may not exist - " + e.message
            end
          end
          
          # Create an index
          if @create_index
            puts "Creating index: #{@index_name}"

             # Read mappings
            file_json = open("index.json")
            string_json = file_json.read


            json_new_index = string_json
          
            surl      = @base_url + 'app/' + @cis_app_name + '/appindices/' + @index_name
            headers   = {:'x-auth-token' => @token} 
            body    = json_new_index
            begin
              response  = RestClient::Request.execute(:method => :post, :url => surl, :payload => body, :content_type => :json, :accept => :json, :verify_ssl => false, :headers => headers)
              puts "Index created"
            rescue Exception => e
              puts "Failed to create index, may already exist - " + e.message
            end
          end
               
    end

    def get_es_stats()

      url = "http://" + @host + ":9200" + "/_stats"

      begin
        response  = RestClient::Request.execute(:method => :get, 
                                                  :url => url,
                                                  :content_type => :json,
                                                  :accept => :json,
                                                  :verify_ssl => false,
                                                  :timeout => 1000)

        response = JSON.parse(response)
        mash = Hashie::Mash.new response




         # Display results
        if mash != nil
          # Calc index stats, display, and save for csv file

          docs_count = mash["_all"]["primaries"]["docs"]["count"]
          stats_docs = docs_count.to_s
          #all_results = all_results + "," + stats_docs

          size_b = mash["_all"]["primaries"]["store"]["size_in_bytes"]
          stats_size = size_b.to_s
          size_mb = mash["_all"]["primaries"]["store"]["size_in_bytes"]  / 1024 / 1024
          size_gb = size_mb / 1024.0
          stats_size_mb = size_mb.to_s
          stats_size_gb = sprintf( "%0.02f", size_gb)
          #all_results = all_results + "," + stats_size_gb

          ave_size = size_b.fdiv(docs_count)
          stats_ave_size = sprintf( "%0.02f", ave_size)
          #all_results = all_results + "," + stats_ave_size

          stats_shards = mash._shards.total.to_s + "/" + mash._shards.successful.to_s + "/" + mash._shards.failed.to_s
          #all_results = all_results + "," + stats_shards

          puts "docs: " + stats_docs
          puts "size: " + stats_size + " (" + stats_size_mb + "MB)" + " (" + stats_size_gb + "GB)"
          puts "doc ave size: " + stats_ave_size
          puts "shards: " + stats_shards
        end


      rescue Exception => e
        puts "Failed to get index statistics - " + e.message
      end

    end 
    
   def generateQueryLocation(docCount)
          
           bulk_plain = ""
           doc = Array.new
           Faker::Metadata.load_strings
           (1..docCount).each do |i|
            
            
           
            doc[i-1] = Faker::Metadata.get_path_isilon 
            
            doc.push(doc[i-1])
            #puts doc
        end
        return doc
      end
    
    
    def query_basic(query_string, body)
      
         puts 
         puts "#{query_string}"
         puts "---------------------"
         
         headers   = {:'x-auth-token' => @token,:'Content-Type' => 'application/json',:'Accept' => 'application/json'}
         
         starttime = Time.now
        begin
          response = RestClient::Request.execute(:method => :post, :url => @surl, :payload => body, :content_type => :json, :accept => :json, :verify_ssl => false, :headers => headers, :timeout => 900)
        rescue => e
          puts "Failed to execute query: " + e.message
          return
        end
        time_taken = Time.now - starttime
        json = JSON.parse response

        # Display results
        mash = Hashie::Mash.new json
        if mash != nil && mash.hits != nil
          es_time = mash.took/1000.0
          hits  = mash.hits.total

          puts "Hits: " + hits.to_s
          puts "ES took: " + es_time.to_s
          puts "REST time: #{time_taken} s"
          puts "Timed_out: " + mash.timed_out.to_s  
        else
          puts "No hits"
        end 
      
    end
    
    def location_query_body(count)
      
        docLocation_array = Array.new
        str_body = String.new
        body =  String.new
       
        docLocation_array = generateQueryLocation(count)
        
        #puts docLocation_array.length
        #puts "#{docLocation_array[0].to_s}"
        
        (1..docLocation_array.length).each do |i|
        if i < docLocation_array.length
        str_body = "\{\"prefix\": \{\"xvlocation.raw\": \"#{docLocation_array[i-1].to_s}\/\"}\}\,\{\"term\": \{\"xvlocation.raw\": \"#{docLocation_array[i-1].to_s}\"}\}\,"
        else 
        str_body = "\{\"prefix\": \{\"xvlocation.raw\": \"#{docLocation_array[i-1].to_s}\/\"}\}\,\{\"term\": \{\"xvlocation.raw\": \"#{docLocation_array[i-1].to_s}\"}\}"
        end  
          #puts str_body
          body << str_body
          
        end
        return body
    end

    def location_query_one_hundred()

        rawbody =  String.new
        rawbody = location_query_body(100)
        #puts rawbody
        
        json_query = "
          \{
            \"query\": \{
              \"filtered\": \{
              \"query\": \{
                \"bool\": \{
                  \"must\": \[\]
                \}
              \}\,
            \"filter\": \{
                \"bool\": \{
                  \"should\": \[
                   
                      #{rawbody}
                    
                 
                  \]
                \}
              \}
            \}
          \}\,
          \"sort\": \[
            \{
              \"_score\": \"desc\"
            \}
          \]\,
          \"from\": 0\,
          \"size\": 10\,
          \"fields\": \[
            \"xvlocation\"\,
            \"xvsize\"\,
            \"xvtype\"\,
            \"xvobjecttype\"\,
            \"xvdate\"\,
            \"xvname\"\,
            \"isi_source_id\"\,
            \"xvsource\"\,
            \"isi_create_time\"\,
            \"isi_access_mode\"\,
            \"isi_owner\"\,
            \"isi_group\"\,
            \"xvcontent\"\,
            \"xvmetadata\"
          \]
             \}"
             
        arr = JSON(json_query)
        body = JSON.generate(arr)
        
        query_basic("100 location raw search", body) 

       
    end
    
    
    def location_query_one_thousand()

        rawbody =  String.new
        rawbody = location_query_body(500)
        #puts rawbody
        
        json_query = "
          \{
            \"query\": \{
              \"filtered\": \{
              \"query\": \{
                \"bool\": \{
                  \"must\": \[\]
                \}
              \}\,
            \"filter\": \{
                \"bool\": \{
                  \"should\": \[
                   
                      #{rawbody}
                    
                 
                  \]
                \}
              \}
            \}
          \}\,
          \"sort\": \[
            \{
              \"_score\": \"desc\"
            \}
          \]\,
          \"from\": 0\,
          \"size\": 10\,
          \"fields\": \[
            \"xvlocation\"\,
            \"xvsize\"\,
            \"xvtype\"\,
            \"xvobjecttype\"\,
            \"xvdate\"\,
            \"xvname\"\,
            \"isi_source_id\"\,
            \"xvsource\"\,
            \"isi_create_time\"\,
            \"isi_access_mode\"\,
            \"isi_owner\"\,
            \"isi_group\"\,
            \"xvcontent\"\,
            \"xvmetadata\"
          \]
             \}"
             
        arr = JSON(json_query)
        body = JSON.generate(arr)
        
        query_basic("500 location raw search", body) 

       
    end
        

end

def run()
    
    # Override RestClient to use timeout (900 seconds)
    query = nil
    query = CISQuery.new()
  
    # Read optional values
    options = {}
    OptionParser.new do |opts|
      opts.banner = "Usage: query_types.rb [options]"
    
      opts.on('-h', '--host HOST', 'ElasticSearch host') { |v| query.host = v }
      opts.on('-p', '--port PORT', 'ElasticSearch port') { |v| query.port = v }

      opts.on('-a', '--app_name NAME', 'Name of CIS application (defaults to test_app)') { |v| query.cis_app_name = v }
      opts.on('-i', '--index_name NAME', 'Name of Index (defaults to ingest)') { |v| query.index_name = v }   

      opts.on('-v', '--cis_ver VER', 'Specific CIS version (defaults to "v2.0")') { |v| query.cis_ver = v }
      opts.on('-u', '--cis_user USER', 'CIS Username (defaults to adminsitrator@domain.com)') { |v| query.cis_username = v }
      opts.on('-w', '--cis_pw PASSWORD', 'CIS Password (defaults to qampass1!)') { |v| query.cis_password = v }

    end.parse!
  

    query.admin_login()
    query.verify_application()
    query.get_es_stats()


    puts Time.now()
    
    query.location_query_one_hundred()
    query.location_query_one_thousand()
    
    puts Time.now()

    puts
    puts "end of testing."
    
end

if __FILE__ == $0
    run()
end

