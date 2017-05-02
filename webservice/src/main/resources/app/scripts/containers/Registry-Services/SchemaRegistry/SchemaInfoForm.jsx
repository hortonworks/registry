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

import React, {Component} from 'react';
import ReactDOM from 'react-dom';
import Select from 'react-select';
import ReactCodemirror from 'react-codemirror';
import CodeMirror from 'codemirror';
import 'codemirror/mode/javascript/javascript';
import jsonlint from 'jsonlint';
import lint from 'codemirror/addon/lint/lint';
import '../../../utils/Overrides';
import Utils from '../../../utils/Utils';
import FSReactToastr from '../../../components/FSReactToastr';
import CommonNotification from '../../../utils/CommonNotification';
import {toastOpt} from '../../../utils/Constants';
import SchemaREST from '../../../rest/SchemaREST';

export default class SchemaFormContainer extends Component {
  constructor(props) {
    super(props);
    this.state = {
      name: '',
      compatibility: 'BACKWARD',
      compatibilityArr: [
        {
          value: 'BACKWARD',
          label: 'BACKWARD'
        }, {
          value: 'FORWARD',
          label: 'FORWARD'
        }, {
          value: 'BOTH',
          label: 'BOTH'
        }, {
          value: 'NONE',
          label: 'NONE'
        }
      ],
      evolve: true,
      schemaText: '',
      schemaTextFile: null,
      type: 'avro',
      typeArr: [],
      schemaGroup: 'Kafka',
      description: '',
      showError: false,
      changedFields: []
    };
    this.fetchData();
  }
  fetchData() {
    SchemaREST.getSchemaProviders().then((results) => {
      this.setState({typeArr: results.entities});
    });
  }

  handleValueChange(e) {
    let obj = {};
    obj[e.target.name] = e.target.value;
    this.setState(obj);
  }

  handleTypeChange(obj) {
    if (obj) {
      this.setState({type: obj.type});
    } else {
      this.setState({type: ''});
    }
  }

  handleCompatibilityChange(obj) {
    if (obj) {
      this.setState({compatibility: obj.value});
    } else {
      this.setState({compatibility: ''});
    }
  }

  handleJSONChange(json){
    this.setState({
      schemaText: json
    });
  }

  handleOnDrop(e) {
    e.preventDefault();
    e.stopPropagation();
    if (!e.dataTransfer.files.length) {
      this.setState({schemaTextFile: null});
    } else {
      var file = e.dataTransfer.files[0];
      var reader = new FileReader();
      reader.onload = function(e) {
        if(Utils.isValidJson(reader.result)) {
          this.setState({schemaTextFile: file, schemaText: reader.result});
        } else {
          this.setState({schemaTextFile: null, schemaText: ''});
        }
      }.bind(this);
      reader.readAsText(file);
    }
  }

  handleToggleEvolve(e) {
    this.setState({evolve: e.target.checked});
  }

  validateData() {
    let {name, type, schemaGroup, description, changedFields, schemaText} = this.state;
    if (name.trim() === '' || schemaGroup === '' || type === '' || description.trim() === '' || schemaText.trim() === '' || !Utils.isValidJson(schemaText.trim())) {
      if (name.trim() === '' && changedFields.indexOf("name") === -1) {
        changedFields.push("name");
      };
      if (schemaGroup.trim() === '' && changedFields.indexOf("schemaGroup") === -1) {
        changedFields.push("schemaGroup");
      }
      if (type.trim() === '' && changedFields.indexOf("type") === -1) {
        changedFields.push("type");
      }
      if (description.trim() === '' && changedFields.indexOf("description") === -1) {
        changedFields.push("description");
      }
      this.setState({showError: true, changedFields: changedFields});
      return false;
    } else {
      this.setState({showError: false});
      return true;
    }
  }

  handleSave() {
    let data = {};
    let {name, type, schemaGroup, description, compatibility, evolve, schemaText} = this.state;
    data = {
      name,
      type,
      schemaGroup,
      description,
      evolve
    };
    if (compatibility !== '') {
      data.compatibility = compatibility;
    }
    return SchemaREST.postSchema({body: JSON.stringify(data)})
        .then((schemaResult)=>{
          if(schemaResult.responseMessage !== undefined){
            FSReactToastr.error(<CommonNotification flag="error" content={schemaResult.responseMessage}/>, '', toastOpt);
          } else {
            let versionData = { schemaText, description };
            return SchemaREST.postVersion(name, {body: JSON.stringify(versionData)});
          }
        });
  }

  render() {
    const jsonoptions = {
      lineNumbers: true,
      mode: "application/json",
      styleActiveLine: true,
      gutters: ["CodeMirror-lint-markers"],
      lint: true
    };
    let {showError, changedFields} = this.state;
    return (
      <form className="form-horizontal">
        <div className="form-group">
          <label>Name <span className="text-danger">*</span></label>
          <div>
            <input name="name" placeholder="Name" onChange={this.handleValueChange.bind(this)} type="text" className={showError && changedFields.indexOf("name") !== -1 && this.state.name.trim() === ''
              ? "form-control invalidInput"
              : "form-control"} value={this.state.name} required={true}/>
          </div>
        </div>
        <div className="form-group">
          <label>description <span className="text-danger">*</span></label>
          <div>
            <input name="description" placeholder="Description" onChange={this.handleValueChange.bind(this)} type="text" className={showError && changedFields.indexOf("description") !== -1 && this.state.description.trim() === ''
              ? "form-control invalidInput"
              : "form-control"} value={this.state.description} required={true}/>
          </div>
        </div>
        <div className="form-group">
          <label>Type <span className="text-danger">*</span></label>
          <div>
            <Select value={this.state.type} options={this.state.typeArr} onChange={this.handleTypeChange.bind(this)} valueKey="type" labelKey="name"/>
          </div>
        </div>
        <div className="form-group">
          <label>Schema Group <span className="text-danger">*</span></label>
          <div>
            <input name="schemaGroup" placeholder="Schema Group" onChange={this.handleValueChange.bind(this)} type="text" className={showError && changedFields.indexOf("schemaGroup") !== -1 && this.state.schemaGroup === ''
              ? "form-control invalidInput"
              : "form-control"} value={this.state.schemaGroup} required={true}/>
          </div>
        </div>
        <div className="form-group">
          <label>Compatibility</label>
          <div>
            <Select value={this.state.compatibility} options={this.state.compatibilityArr} onChange={this.handleCompatibilityChange.bind(this)} clearable={false}/>
          </div>
        </div>
        <div className="form-group">
          <div className="checkbox">
          <label><input name="evolve" onChange={this.handleToggleEvolve.bind(this)} type="checkbox" value={this.state.evolve} checked={this.state.evolve}/> Evolve</label>
        </div>
      </div>
      <div className="form-group">
        <label>Schema Text <span className="text-danger">*</span>&nbsp;<span style={{textTransform: 'none'}}>(Type or Drop a file)</span></label>
        <div onDrop={this.handleOnDrop.bind(this)}>
          <ReactCodemirror ref="JSONCodemirror" value={this.state.schemaText} onChange={this.handleJSONChange.bind(this)} options={jsonoptions} />
        </div>
      </div>
      </form>
    );
  }
}
