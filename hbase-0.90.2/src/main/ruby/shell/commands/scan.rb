#
# Copyright 2010 The Apache Software Foundation
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

module Shell
  module Commands
    class Scan < Command
      def help
        return <<-EOF
Scan a table; pass table name and optionally a dictionary of scanner
specifications.  Scanner specifications may include one or more of
the following: LIMIT, STARTROW, STOPROW, TIMESTAMP, or COLUMNS.  If
no columns are specified, all columns will be scanned.  To scan all
members of a column family, leave the qualifier empty as in
'col_family:'.  Examples:

  hbase> scan '.META.'
  hbase> scan '.META.', {COLUMNS => 'info:regioninfo'}
  hbase> scan 't1', {COLUMNS => ['c1', 'c2'], LIMIT => 10, STARTROW => 'xyz'}

For experts, there is an additional option -- CACHE_BLOCKS -- which
switches block caching for the scanner on (true) or off (false).  By
default it is enabled.  Examples:

  hbase> scan 't1', {COLUMNS => ['c1', 'c2'], CACHE_BLOCKS => false}
EOF
      end

      def command(table, args = {})
        now = Time.now
        formatter.header(["ROW", "COLUMN+CELL"])

        count = table(table).scan(args) do |row, cells|
          formatter.row([ row, cells ])
        end

        formatter.footer(now, count)
      end
    end
  end
end
