--[[

    Copyright © 2020 ProStore

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

]]
vshard = require'vshard'

load_lines('test_shares_accounts_actual',
    { {'C', 1, 1, 0, box.NULL}
    , {'D', 2, 1, 0, box.NULL}
    , {'D', 3, 1, 0, box.NULL}
})

load_lines('test_shares_transactions_actual',
    { {'2020-06-11', -100, 1, 1, 0, 0, box.NULL, vshard.router.bucket_id(1)}
    , {'2020-06-11', 100, 1, 2, 0, 0, box.NULL, vshard.router.bucket_id(2)}
})

load_lines('test_shares_transactions_actual',
    { {'2020-06-12', -50, 2, 2, 0, 1, box.NULL, vshard.router.bucket_id(2)}
    , {'2020-06-12', 50, 2, 3, 0, 1, box.NULL, vshard.router.bucket_id(3)}
})

load_lines('test_shares_transactions_actual',
    { {'2020-06-12', -20, 3, 3, 0, 1, box.NULL, vshard.router.bucket_id(3)}
    , {'2020-06-12', 20, 3, 1, 0, 1, box.NULL, vshard.router.bucket_id(1)}
})