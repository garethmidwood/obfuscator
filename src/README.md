# Database Obfuscator
A tool to retrieve database dumps containing PII, to obfuscate data in specified fields and to output a cleansed database dump for use in development/testing environments.


## The process
This tool will create a database, import the backup pulled from the source storage location, obfuscate the data as defined in the manifest, dump the cleansed database and push to the destination storage location.
The original database will be removed _locally_, the file will remain in the initial location. 


## obfuscate.yml - Tool Configuration
Your tool requires configuration, this is an `obfuscate.yml` file that should be added in the same directory as this README.

This tool can be configured to pull from multiple `source` locations. The credentials used should give permission to read from the location and to delete the manifest.yml file once processing is completed.

The `database` must be configured and the user must have the ability to create and delete databases.

### obfuscate.yml - Available options
```
# obfuscate.yml

source:
 -
  [location]
 -
  [location]
 -
  [location]
database:
 user: dbusername
 password: dbpassword
 host: dbhost
 port: dbport
```

#### obfuscate.yml - Example
```
source:
  -
   type: S3
   bucket: obfuscated.bucket.name
   access: ABCDEFGHIJKLMNOPQRSTUVWXYZ
   secret: ABCDEFGHIJKLMNOPQRSTUVWXYZ
   region: eu-west-2
  -
   type: S3
   bucket: secondobfuscated.bucket.name
   access: ABCDEFGHIJKLMNOPQRSTUVWXYZ
   secret: ABCDEFGHIJKLMNOPQRSTUVWXYZ
   region: eu-west-2
database:
  user: webuser
  password: webpassword
  host: mysql
  port: 3306
```


## manifest.yml - The Dump Manifest File
When you generate a DB dump for obfuscation you should place alongside it a `manifest.yml` file.

The manifest should be a yml file containing details of the tables and fields to obfuscate, and where to push the cleansed file when completed.

#### manifest.yml - Available Options
```
destination:
  [location] # see Location Types below
data:
  - table_name
    - obfuscation_field_type:
      - field_1
      - field_2
      - ...
    - obfuscation_field_type_2:
      - field_1
      - ...
```

#### manifest.yml - Example
```
destination:
  type: S3
  bucket: your-bucket-name
  access: ABCDEFGHIJKLMNOPQRSTUVWXYZ
  secret: ABCDEFGHIJKLMNOPQRSTUVWXYZ
  region: eu-west-2
  dir: path/to/put/cleansed/db
  filename: obfuscated.sql
data:
  - wp_users:
    - string:
      - user_login
      - user_nicename
      - display_name
    - email:
      - user_email
  - wp_comments:
    - string:
      - comment_author
    - email:
      - comment_author_email
```

## manifest.yml - Obfuscation Field Types
```
string
email
phone number
```




## Location types
Locations refer to external storage for pulling and pushing DB dumps.

Note: `type` is always a required field for both source and destination, it refers to one of the types below.

### Type: S3
#### Example
```
type: S3
bucket: your-bucket-name
access: ABCDEFGHIJKLMNOPQRSTUVWXYZ
secret: ABCDEFGHIJKLMNOPQRSTUVWXYZ
region: eu-west-2
dir: path/to/put/cleansed/db # destination only, defaults to /
filename: obfuscated.sql # destination only, defaults to obfuscated.sql
```

#### Source Required Fields
```
bucket
region
access
secret
```

#### Source Optional Fields
```
none
```

#### Destination Required Fields
```
access
secret
```

#### Destination Optional Fields
```
dir # defaults to /

# the following fields will be retrieved from the manifest.yml file, but fallbacks can be specified here
bucket
region
```


