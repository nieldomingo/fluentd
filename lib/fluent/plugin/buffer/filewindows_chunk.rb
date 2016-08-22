#
# Fluentd
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

require 'fluent/plugin/buffer/file_chunk'

module Fluent
  module Plugin
    class Buffer
      class FileWindowsChunk < FileChunk
        def read
          if @state == :queued
            @chunk = File.open(@path, 'rb+')
            @chunk.set_encoding(Encoding::ASCII_8BIT)
            @chunk.sync = true
            @chunk.binmode
            content = @chunk.read
            @chunk.close
            content
          else
            super
          end
        end

        def load_existing_enqueued_chunk(path)
          super
          @chunk.close
          @state = :queued
        end
        
        def enqueued!
          super
          @chunk.close
          @meta.close
        end
        
        def open(&block)
          if @chunk.closed?
            @chunk = File.open(@path, 'rb+')
            @chunk.set_encoding(Encoding::ASCII_8BIT)
            @chunk.sync = true
            @chunk.binmode
          end
          super
        end
        
        def close
          if @chunk.closed?
            @chunk = File.open(@path, 'rb+')
            @chunk.set_encoding(Encoding::ASCII_8BIT)
            @chunk.sync = true
            @chunk.binmode
          end
          size = @chunk.size
          @chunk.close
          @meta.close if @meta # meta may be missing if chunk is queued at first
          if size == 0
            File.unlink(@path, @meta_path)
          end
        end

        def purge
          @chunk.close unless @chunk.closed?
          @meta.close if @meta and not @meta.closed?
          @bytesize = @size = @adding_bytes = @adding_size = 0
          File.unlink(@path, @meta_path)
        end
      end
    end
  end
end
