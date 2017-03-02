/**
  * Copyright 2017 Hortonworks.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *   http://www.apache.org/licenses/LICENSE-2.0
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
**/

import fetch from 'isomorphic-fetch';
import {baseUrl} from '../utils/Constants';
import {CustomFetch} from '../utils/Overrides';

const SchemaREST = {
  postSchema(options) {
    options = options || {};
    options.method = options.method || 'POST';
    options.headers = options.headers || {
      'Content-Type': 'application/json',
      'Accept': 'application/json'
    };
    return fetch(baseUrl + 'schemaregistry/schemas', options)
      .then((response) => {
        return response.json();
      });
  },
  getAllSchemas(options) {
    options = options || {};
    options.method = options.method || 'GET';
    return fetch(baseUrl + 'schemaregistry/schemas', options)
      .then((response) => {
        return response.json();
      });
  },
  getSchemaInfo(name, options) {
    options = options || {};
    options.method = options.method || 'GET';
    name = encodeURIComponent(name);
    return fetch(baseUrl + 'schemaregistry/schemas/' + name, options)
      .then((response) => {
        return response.json();
      });
  },
  getSchemaVersions(name, options) {
    options = options || {};
    options.method = options.method || 'GET';
    name = encodeURIComponent(name);
    return fetch(baseUrl + 'schemaregistry/schemas/' + name + '/versions', options)
      .then((response) => {
        return response.json();
      });
  },
  getLatestVersion(name, options) {
    options = options || {};
    options.method = options.method || 'GET';
    name = encodeURIComponent(name);
    return fetch(baseUrl+'schemaregistry/schemas/'+name+'/versions/latest', options)
      .then( (response) => {
        return response.json();
      });
  },
  postVersion(name, options) {
    options = options || {};
    options.method = options.method || 'POST';
    options.headers = options.headers || {
      'Content-Type': 'application/json',
      'Accept': 'application/json'
    };
    name = encodeURIComponent(name);
    return fetch(baseUrl + 'schemaregistry/schemas/' + name + '/versions', options)
      .then((response) => {
        return response.json();
      });
  },
  getSchemaProviders(options) {
    options = options || {};
    options.method = options.method || 'GET';
    return fetch(baseUrl + 'schemaregistry/schemaproviders', options)
      .then((response) => {
        return response.json();
      });
  },
  getCompatibility(name, options) {
    options = options || {};
    options.method = options.method || 'POST';
    options.headers = options.headers || {
      'Content-Type' : 'application/json',
      'Accept' : 'application/json'
    };
    name = encodeURIComponent(name);
    return fetch(baseUrl+'schemaregistry/schemas/'+name+'/compatibility', options)
      .then( (response) => {
        return response.json();
      });
  }
};

export default SchemaREST;
