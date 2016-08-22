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

require 'fluent/plugin/buf_file'
require 'fluent/plugin/buffer/filewindows_chunk'

module Fluent
  module Plugin
    class FileWindowsBuffer < Fluent::Plugin::FileBuffer
      Plugin.register_buffer('filewindows', self)
      
      def resume
        stage = {}
        queue = []

        Dir.glob(@path) do |path|
          m = new_metadata() # this metadata will be overwritten by resuming .meta file content
                             # so it should not added into @metadata_list for now
          mode = Fluent::Plugin::Buffer::FileWindowsChunk.assume_chunk_state(path)
          if mode == :unknown
            log.debug "uknown state chunk found", path: path
            next
          end

          chunk = Fluent::Plugin::Buffer::FileWindowsChunk.new(m, path, mode) # file chunk resumes contents of metadata
          case chunk.state
          when :staged
            stage[chunk.metadata] = chunk
          when :queued
            queue << chunk
          end
        end

        queue.sort_by!{ |chunk| chunk.modified_at }

        return stage, queue
      end

      def generate_chunk(metadata)
        # FileChunk generates real path with unique_id
        if @file_permission
          Fluent::Plugin::Buffer::FileWindowsChunk.new(metadata, @path, :create, perm: @file_permission)
        else
          Fluent::Plugin::Buffer::FileWindowsChunk.new(metadata, @path, :create)
        end
      end
    end
  end
end
