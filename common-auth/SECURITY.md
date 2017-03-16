# Running registry and streamline web-services securely

This module is intended to be used by registry and streamline web-services so that they can enable http client authentication via SPNEGO. Currently supported 
authentication mechanism is Kerberos. The code for this module has been borrowed from hadoop-auth(2.7.3) module in hadoop project and changed a little. The 
reasons for doing so are to avoid having a dependency on hadoop-auth module which brings in some other modules, avoid conflicts with other versions of 
hadoop-auth module and having more control over the changes needed in future. Some text for this document has been borrowed from SECURITY.md of Apache Storm  

By default, registry and streamline web-services are running without authentication enabled and anyone can access the web-services from ui/client as far as they
know the url and can access the web-server from the client machine. To enable authentication with the client, webservice needs to add a servlet filter from 
this module. The webservice module will need to declare a dependency on this module. One way of adding a servlet filter in code is as follows. 

```java
List<ServletFilterConfiguration> servletFilterConfigurations = registryConfiguration.getServletFilters();
if (servletFilterConfigurations != null && !servletFilterConfigurations.isEmpty()) {
    for (ServletFilterConfiguration servletFilterConfiguration: servletFilterConfigurations) {
        try {
            FilterRegistration.Dynamic dynamic = environment.servlets().addFilter(servletFilterConfiguration.getClassName(), (Class<? extends Filter>)
            Class.forName(servletFilterConfiguration.getClassName()));
            dynamic.setInitParameters(servletFilterConfiguration.getParams());
            dynamic.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");
        } catch (Exception e) {
            LOG.error("Error registering servlet filter {}", servletFilterConfiguration);
            throw new RuntimeException(e);
        }
    }
}
```

ServletFilterConfiguration is a java object representing any servlet filter used in schema registry. However the general idea is that one needs to
add com.hortonworks.registries.auth.server.AuthenticationFilter for enabling authentication 

The configuration for that filter in yaml is represented in params property as follows.

```yaml
servletFilters:
 - className: "com.hortonworks.registries.auth.server.AuthenticationFilter"
   params:
     type: "kerberos"
     kerberos.principal: "HTTP/web-service-host.com"
     kerberos.keytab: "/path/to/keytab"
     kerberos.name.rules: "RULE:[2:$1@$0]([jt]t@.*EXAMPLE.COM)s/.*/$MAPRED_USER/ RULE:[2:$1@$0]([nd]n@.*EXAMPLE.COM)s/.*/$HDFS_USER/DEFAULT"
     token.validity: 36000
```

The servlet filter uses the principal `HTTP/{hostname}` (here hostname should be the host where the web-service runs) to login. Make sure that principal is 
created as part of Kerberos setup

Once configured, you must do `kinit` on client side using the principal of the user before accessing the web-service via the browser or some other client. 
This principal also needs to be created first during Kerberos setup

Here's an example of accessing web-service after the setup above:
```bash
curl  -i --negotiate -u:anyUser  -b ~/cookiejar.txt -c ~/cookiejar.txt  http://<web-service-host>:<port>/api/v1/
```

1. Firefox: Go to `about:config` and search for `network.negotiate-auth.trusted-uris` double-click to add value "http://<web-service-host>:<port>"
2. Google-chrome: start from command line with: 
   `google-chrome --auth-server-whitelist="*web-service-hostname" --auth-negotiate-delegate-whitelist="*web-service-hostname"`
3. IE: Configure trusted websites to include "web-service-hostname" and allow negotiation for that website

**Caution**: In AD MIT Kerberos setup, the key size is bigger than the default UI jetty server request header size. If using that, make sure you set 
header buffer bytes to 65536



