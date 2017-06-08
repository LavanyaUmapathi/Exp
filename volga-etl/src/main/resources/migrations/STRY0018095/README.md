Steps to follow after deployment
Story number: STRY0018095

1.Login to prod server
ssh -A <sso>@admin1.prod.iad.caspian.rax.io

2.The maas account load is scheduled to start at 3PM UTC [9 AM CST] [8:30 PM IST]. So, these steps to be performed an hour before i.e., 2PM UTC [8 AM CST] [7:30 PM IST].

Manual Deployment Steps:
Clone the git repo
1. git clone git@github.com:racker/volga.git

2. Copy the accoutn load script
   cd volga/hadoopv1-pipeline/maas-etl
   sudo cp maas-account-load.sh /data/acumen-admin/maas-etl/

3. Copy hql scripts
   cd 
   cd volga/volga-etl/src/main/resources/schemas/maas/
   sudo cp accounts.hql /usr/local/acumen-etl/classes/schemas/maas
   sudo cp checks.hql /usr/local/acumen-etl/classes/schemas/maas
   sudo cp entities.hql /usr/local/acumen-etl/classes/schemas/maas


Steps to follow after deployment

1. Switch to maas user
sudo su - maas

2. Make a deployment folder
mkdir STRY0018095

3. Copy deployment files to this folder 
   exit
   cd volga/volga-etl/src/main/resources/migrations/STRY0018095
   sudo cp * /home/maas/STRY0018095
   ls
files maas_accounts_tables_update.sh and update_maas_account_tables.hql should be present 

4. Check for update in the following files in the path 
/usr/local/acumen-etl/classes/schemas/maas
 -> accounts.hql
 -> checks.hql
 -> entities.hql
They should have insert statement for history table in below manner

INSERT OVERWRITE TABLE accounts_history
PARTITION (dt)
SELECT
    *
  , from_unixtime(unix_timestamp() ,  'yyyy-MM-dd')
FROM accounts_stg;

5. Run the shell script
   sudo su - maas
   cd /home/maas/STRY0018095
   sh maas_accounts_tables_update.sh

6. To verify go to hive and check the tables 
hive
USE maas;
describe formatted accounts_history;
describe formatted checks_history;
describe formatted entities_history;

expected result: table should be created and should have partition defined


