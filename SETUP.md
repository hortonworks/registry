#Schema Registry Setup

## CentOS/RedHat
1.  Install Java
	
	```
	# wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u111-b14/jdk-8u111-linux-x64.tar.gz"
	#  tar xzf jdk-8u111-linux-i586.tar.gz
	# cd /opt/jdk1.8.0_111/
	# alternatives --install /usr/bin/java java /opt/jdk1.8.0_111/bin/java 2
	# alternatives --config java
	```

2. edit $REGISTRY_HOME/conf/registry-env.sh, add the following

	```
		export JAVA_HOME=/opt/jdk1.8.0_111/
	```

3. Setup Database

	### 3.1 Mysql
	
	####Install Mysql
	```	
	sudo yum install mysql-server
	sudo mysql-server start
	```
	#### Cofigure Mysql
	```
	 # set root password
    sudo mysql_secure_installation
	```
	
	#### Setup Database
	```
	mysql -u root -p
	create database registry_db;
	CREATE USER 'registry_user'@'localhost' IDENTIFIED BY 'registry_password';
	GRANT ALL PRIVILEGES ON registry_db.* TO 'registry_user'@'localhost' WITH GRANT OPTION;
	commit;
	```
4. Configure registry.yaml
   
   ```
     cp conf/registry.yaml.mysql.example conf/registry.yaml
   ```
   Edit the folllowing section to add appropriate database and user settings
   
   ```
   storageProviderConfiguration:
     providerClass: "com.hortonworks.registries.storage.impl.jdbc.JdbcStorageManager"
     properties:
   		db.type: "mysql"
   		queryTimeoutInSecs: 30
   		db.properties:
   		  dataSourceClassName: "com.mysql.jdbc.jdbc2.optional.MysqlDataSource"
   		  dataSource.url: "jdbc:mysql://localhost/registry_db"
   		  dataSource.user: "registry_user"
   		  dataSource.password: "registry_password"

   ```
   
5. Start the registry server
  
  ``` sudo ./bin/registry start ```


## OS X

1. Download latest Schema Registry binaries from here https://github.com/hortonworks/registry/releases
2. edit $REGISTRY_HOME/conf/registry-env.sh, add the following

	```
		export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
	```
3. Setup Database

	### 3.1 Mysql
	
	####Install Mysql
	```
	brew install mysql
	launchctl load ~/Library/LaunchAgents/homebrew.mxcl.mysql.plist
	export MYSQL_PATH=/usr/local/Cellar/mysql/5.6.27  
	export PATH=$PATH:$MYSQL_PATH/bin
	```
	#### Configure Mysql
	```
	mysqladmin -u root password 'yourpassword'
	mysql -u root -p
	```
	#### Setup Database
	```
	mysql -u root -p
	create database registry_db;
	CREATE USER 'registry_user'@'localhost' IDENTIFIED BY 'registry_password';
	GRANT ALL PRIVILEGES ON registry_db.* TO 'registry_user'@'localhost' WITH GRANT OPTION;
	commit;
	```

4. Configure registry.yaml
   
   ```
     cp conf/registry.yaml.mysql.example conf/registry.yaml
   ```
   Edit the folllowing section to add appropriate database and user settings
   
   ```
   storageProviderConfiguration:
     providerClass: "com.hortonworks.registries.storage.impl.jdbc.JdbcStorageManager"
     properties:
   		db.type: "mysql"
   		queryTimeoutInSecs: 30
   		db.properties:
   		  dataSourceClassName: "com.mysql.jdbc.jdbc2.optional.MysqlDataSource"
   		  dataSource.url: "jdbc:mysql://localhost/registry_db"
   		  dataSource.user: "registry_user"
   		  dataSource.password: "registry_password"

   ```
   
5. Start the registry server
  
  ``` sudo ./bin/registry start ```
