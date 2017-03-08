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
import ReactCodemirror from 'react-codemirror';
import '../../../utils/Overrides';
import CodeMirror from 'codemirror';
import 'codemirror/mode/javascript/javascript';
import jsonlint from 'jsonlint';
import lint from 'codemirror/addon/lint/lint';
import SchemaREST from '../../../rest/SchemaREST';
import Utils from '../../../utils/Utils';

CodeMirror.registerHelper("lint", "json", function(text) {
  var found = [];
  var {parser} = jsonlint;
  parser.parseError = function(str, hash) {
    var loc = hash.loc;
    found.push({
      from: CodeMirror.Pos(loc.first_line - 1, loc.first_column),
      to: CodeMirror.Pos(loc.last_line - 1, loc.last_column),
      message: str
    });
  };
  try {
    jsonlint.parse(text);
  } catch (e) {}
  return found;
});

export default class SchemaVersionForm extends Component {
  constructor(props) {
    super(props);
    let schemaObj = this.props.schemaObj;
    this.state = {
      schemaText: schemaObj.schemaText || '',
      schemaTextFile: schemaObj.schemaTextFile || null,
      description: schemaObj.description || '',
      validInput: false,
      showFileError: false,
      showError: false,
      changedFields: []
    };
  }

  handleValueChange(e) {
    let obj = {};
    obj[e.target.name] = e.target.value;
    this.setState(obj);
  }

  handleJSONChange(json) {
    this.setState({schemaText: json});
  }

  handleOnFileChange(e) {
    if (!e.target.files.length) {
      this.setState({validInput: false, showFileError: true, schemaTextFile: null});
    } else {
      var file = e.target.files[0];
      var reader = new FileReader();
      reader.onload = function(e) {
        if(Utils.isValidJson(reader.result)) {
          this.setState({validInput: true, showFileError: false, schemaTextFile: file, schemaText: reader.result});
        } else {
          this.setState({validInput: false, showFileError: true, schemaTextFile: null, schemaText: ''});
        }
      }.bind(this);
      reader.readAsText(file);
    }
  }

  validateData() {
    let {schemaText, description, changedFields} = this.state;
    if (schemaText.trim() === '' || !Utils.isValidJson(schemaText.trim()) || description.trim() === '') {
      if (description.trim() === '' && changedFields.indexOf("description") === -1) {
        changedFields.push('description');
      }
      this.setState({showError: true, changedFields: changedFields});
      return false;
    } else {
      this.setState({showError: false});
      return true;
    }
  }

  handleSave() {
    let {schemaText, description} = this.state;
    let data = {
      schemaText,
      description
    };
    return SchemaREST.getCompatibility(this.props.schemaObj.schemaName, {body: JSON.stringify(JSON.parse(schemaText))})
      .then((result)=>{
        if(result.compatible){
          return SchemaREST.postVersion(this.props.schemaObj.schemaName, {body: JSON.stringify(data)});
        } else {
          return result;
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
    let {showError, changedFields, validInput, showFileError} = this.state;
    return (
      <form>
        <div className="form-group">
          <label>Description <span className="text-danger">*</span></label>
          <div>
            <input name="description" placeholder="Description" onChange={this.handleValueChange.bind(this)} type="text" className={showError && changedFields.indexOf("description") !== -1 && this.state.description.trim() === ''
              ? "form-control invalidInput"
              : "form-control"} value={this.state.description} required={true}/>
          </div>
        </div>
        <div className="form-group">
          <label>Upload Schema from file </label>
            <div>
              <input type="file" className={showFileError
              ? "form-control invalidInput"
              : "form-control"} name="files" title="Upload File" onChange={this.handleOnFileChange.bind(this)}/>
            </div>
        </div>
        {validInput ?
        (<div className="form-group">
          <label>Schema Text <span className="text-danger">*</span></label>
          <div>
            <ReactCodemirror ref="JSONCodemirror" value={this.state.schemaText} onChange={this.handleJSONChange.bind(this)} options={jsonoptions}/>
          </div>
        </div>) : ''
        }
      </form>
    );
  }
}
