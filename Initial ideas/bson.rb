# This script takes a Ruby dictionary, converts it to BSON and writes to a file. Can be
# used to generate files to test with the Erlang parser
require 'bson'
require 'json'


# Use to parse JSON to Ruby dictionary
#payload ='''{"hello":"world"}'''
#doc = JSON.parse(payload)

# Sample doc
#doc = {:hello => 'world',:foo => 'bar',:meh => [1,2,3]}

doc = {:hello => {:world => {:meh => "woo!"}}}

bson = BSON.serialize(doc).to_s

File.open("test.bson",'w') { |file| file.write(bson)}

