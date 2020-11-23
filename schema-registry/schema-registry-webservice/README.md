# Schema Registry Webservice

This module is the REST service running inside Dropwizard. 
You will find the REST endpoints here.

## Developer's Guide

You can run a local instance of Schema Registry by running 
the `LocalSchemaRegistryServer` class found within this
module. This class doesn't have a main method, so you need
to add the following code yourself:

```
public static void main(String[] args) throws Exception {
    LocalSchemaRegistryServer server = new LocalSchemaRegistryServer("/path/to/the/registry.yaml");
    server.start();
}
```

Notice how you need to provide a path to your configuration
file.

### Running from IDE

After adding the main() method, you can right-click on the
class and run it, debug it.

Note that in case of certain JVMs you may face difficulties. 
One known issue is Amazon's Corretto which causes an error 
with Javassist. To fix the issue, you need to add the `-noverify`
JVM parameter to your run configuration. 