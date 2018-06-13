# Database Obfuscator
A tool to retrieve database dumps containing PII, to obfuscate data in specified fields and to output a cleansed database dump for use in development/testing environments.


## The process
This tool will create a database, import the backup pulled from the source storage location, obfuscate the data as defined in the manifest, dump the cleansed database and push to the destination storage location.
The original database will be removed _locally_, the file will remain in the initial location. 


## Tool Configuration - obfuscate.yml
This tool can be configured to pull from multiple source locations. 
The database must be configured and the user must have the ability to create and delete databases.

### Available options
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

#### Example
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

## Location types
Note: `type` is always a required field for both source and destination, it refers to one of the types below.

### Type: S3
#### Example
```
type: S3
bucket: your-bucket-name
access: ABCDEFGHIJKLMNOPQRSTUVWXYZ
secret: ABCDEFGHIJKLMNOPQRSTUVWXYZ
region: eu-west-2
dir: path/to/put/clean/db # destination only, defaults to /
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


## The Dump Manifest File
The dump manifest should be a yml file containing details of the tables and fields to obfuscate.

#### Example
```
destination:
  type: S3
  bucket: development.bucket.name # destination bucket to push to
  region: eu-west-2 # region of the above bucket
  access: ABCDEFGHIJKLMNOPQRSTUVWXYZ
  secret: ABCDEFGHIJKLMNOPQRSTUVWXYZ
  dir: path/to/put/cleansed/db
  filename: obfuscated.sql
data:
  - wp_users: # array of db table names
    - string: # array of field types
      - user_login # array of field names
      - user_nicename
      - display_name
    - email:
      - user_email
```

## Obfuscation field types
```
string
email
name
address
phone number
```


