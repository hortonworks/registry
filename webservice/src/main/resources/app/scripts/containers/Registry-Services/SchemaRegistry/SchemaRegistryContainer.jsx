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
import _ from 'lodash';
import BaseContainer from '../../BaseContainer';
import {Link} from 'react-router';
import FSModal from '../../../components/FSModal';
import {
    DropdownButton,
    MenuItem,
    FormGroup,
    InputGroup,
    FormControl,
    Button,
    PanelGroup,
    Panel,
    Modal,
    Pagination,
    OverlayTrigger,
    Popover
} from 'react-bootstrap';
import Utils, {StateMachine} from '../../../utils/Utils';
import ReactCodemirror from 'react-codemirror';
import '../../../utils/Overrides';
import CodeMirror from 'codemirror';
import 'codemirror/mode/javascript/javascript';
import jsonlint from 'jsonlint';
import lint from 'codemirror/addon/lint/lint';
import SchemaInfoForm from './SchemaInfoForm';
import SchemaVersionForm from './SchemaVersionForm';
import SchemaVersionDiff from './SchemaVersionDiff';
import FSReactToastr from '../../../components/FSReactToastr';
import SchemaREST from '../../../rest/SchemaREST';
import NoData from '../../../components/NoData';
import {toastOpt} from '../../../utils/Constants';
import CommonNotification from '../../../utils/CommonNotification';

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

class ChangeState extends Component{
  constructor(props){
    super(props);
    this.state = {
      edit: false
    };
  }
  changeState(e){
    const {version} = this.props;
    SchemaREST.changeStateOfVersion(version.id, this.refs.stateSelect.value, {}).then((res) => {
      version.stateId = parseInt(this.refs.stateSelect.value);
      this.setState({edit: false});
    });
  }
  onEdit(){
    this.setState({edit: true}, () => {});
  }
  render(){
    const {edit} = this.state;
    const {StateMachine, version, showEditBtn} = this.props;
    const transitions = StateMachine.getTransitionStateOptions(version.stateId);
    const currentState = StateMachine.getStateById(version.stateId).name;
    let comp;
    if(edit){
      comp = <div style={{"marginTop": "5px"}}>
        <select ref="stateSelect" className="stateSelect" defaultValue={version.stateId}>
          <option disabled value={version.stateId}>{currentState}</option>
          {transitions.map( option =>
            (<option value={option.targetStateId} key={option.targetStateId}>{option.name}</option>)
          )}
        </select>
        &nbsp;
        <a href="javascript:void(0)" className="btn-stateSelect" onClick={this.changeState.bind(this)}>
          <i className="fa fa-check" aria-hidden="true"></i>
        </a>
        &nbsp;
        <a href="javascript:void(0)" className="btn-stateSelect" onClick={() => this.setState({edit: false})}>
          <i className="fa fa-times" aria-hidden="true"></i>
        </a>
      </div>;
    }else{
      comp = <div>
          <span className="text-muted">{currentState}</span>
          &nbsp;
          {transitions.length &&  showEditBtn?
          <a href="javascript:void(0)" onClick={this.onEdit.bind(this)}><i className="fa fa-pencil" aria-hidden="true"></i></a>
          : ''
          }
        </div>;
    }
    return comp;
  }
}

export default class SchemaRegistryContainer extends Component {
  constructor(props) {
    super();
    this.breadcrumbData = {
      title: 'Schema Registry',
      linkArr: [
        {
          title: 'Registry Service'
        }, {
          title: 'Schema Registry'
        }
      ]
    };

    this.state = {
      modalTitle: '',
      schemaData: [],
      slideInput : false,
      filterValue: '',
      fetchLoader: true,
      sorted : {
        key : 'timestamp',
        text : 'Last Updated'
      },
      expandSchema: false,
      activePage: 1,
      pageSize: 10,
      dataFound: true
    };
    this.schemaObj = {};
    this.schemaText = '';

    this.StateMachine = new StateMachine();
  }
  componentDidUpdate(){
    this.btnClassChange();
  }
  componentDidMount(){
    this.btnClassChange();
    this.fetchData().then((entities) => {
      if(!entities.length){
        this.setState({dataFound: false});
      }
    });
    this.fetchStateMachine();
  }
  btnClassChange = () => {
    if(!this.state.loading){
      if(this.state.schemaData.length !== 0){
        const sortDropdown = document.querySelector('.sortDropdown');
        sortDropdown.setAttribute("class","sortDropdown");
        sortDropdown.parentElement.setAttribute("class","dropdown");
        if(this.refs.schemaName && this.schemaNameTagWidth != this.refs.schemaName.offsetWidth){
          this.schemaNameTagWidth = this.refs.schemaName.offsetWidth;
          this.schemaGroupTagWidth= this.refs.schemaGroup.offsetWidth;
          this.setState(this.state);
        }
      }
    }
  }
  fetchStateMachine(){
    return SchemaREST.getSchemaVersionStateMachine().then((res) => {
      this.StateMachine.setStates(res);
    });
  }
  fetchData() {
    let promiseArr = [],
      schemaData = [],
      schemaCount = 0;

    const {filterValue} = this.state;
    const {key} = this.state.sorted;
    const sortBy = (key === 'name') ? key+',a' : key+',d';

    this.setState({loading: true});
    return SchemaREST.searchAggregatedSchemas(sortBy, filterValue).then((schema) => {
      let schemaEntities = [];
      if (schema.responseMessage !== undefined) {
        FSReactToastr.error(
          <CommonNotification flag="error" content={schema.responseMessage}/>, '', toastOpt);
      } else {
        schema.entities.map((obj, index) => {
          let {name, schemaGroup, type, description, compatibility, evolve} = obj.schemaMetadata;
          schemaCount++;

          const versionsArr = [];
          let currentVersion = 0;
          if(obj.versions.length){
            obj.versions.forEach((v) => {
              versionsArr.push({
                versionId: v.version,
                description: v.description,
                schemaText: v.schemaText,
                schemaName: name,
                timestamp: v.timestamp,
                stateId: v.stateId,
                id: v.id
              });
            });
            currentVersion = Utils.sortArray(obj.versions.slice(), 'timestamp', false)[0].version;
          }

          schemaData.push({
            id: (index + 1),
            type: type,
            compatibility: compatibility,
            schemaName: name,
            schemaGroup: schemaGroup,
            evolve: evolve,
            collapsed: true,
            versionsArr:  versionsArr,
            currentVersion: currentVersion,
            serDesInfos: obj.serDesInfos
          });
          schemaEntities = schemaData;
        });
        let dataFound = this.state.dataFound;
        if(!dataFound && schemaEntities.length){
          dataFound = true;
        }
        this.setState({schemaData: schemaEntities, loading: false, dataFound: dataFound});
        if(schema.entities.length === 0) {
          this.setState({loading: false});
        }
      }
      return schemaEntities;
    });
  }
  getIconClass(c) {
    switch(c){
    case 'FORWARD':
      return "fa fa-arrow-right";
    case 'BACKWARD':
      return "fa fa-arrow-left";
    case 'BOTH':
      return "fa fa-exchange";
    case 'NONE':
      return "fa fa-ban";
    default:
      return '';
    }
  }
  getBtnClass(c) {
    switch(c){
    case 'FORWARD':
      return "warning";
    case 'BACKWARD':
      return "backward";
    case 'BOTH':
      return "";
    default:
      return 'default';
    }
  }
  slideInput = (e) => {
    this.setState({slideInput  : true});
    const input = document.querySelector('.inputAnimateIn');
    input.focus();
  }
  slideInputOut = () =>{
    const input = document.querySelector('.inputAnimateIn');
    (_.isEmpty(input.value)) ? this.setState({slideInput  : false}) : '';
  }
  onFilterChange = (e) => {
    this.setState({filterValue: e.target.value});
  }
  onFilterKeyPress = (e) => {
    if(e.key=='Enter'){
      this.setState({filterValue: e.target.value.trim(), activePage: 1}, () => {
        this.fetchData();
      });
    }
  }
  filterSchema(entities, filterValue){
    let matchFilter = new RegExp(filterValue , 'i');
    return entities.filter(e => !filterValue || matchFilter.test(e.schemaName));
  }
  onSortByClicked = (eventKey,el) => {
    const liList = el.target.parentElement.parentElement.children;
    let {schemaData} = this.state;
    for(let i = 0;i < liList.length ; i++){
      liList[i].setAttribute('class','');
    }
    el.target.parentElement.setAttribute("class","active");
    //if sorting by name, then in ascending order
    //if sorting by timestamp, then in descending order

    const sortObj = {key : eventKey , text : this.sortByKey(eventKey)};
    this.setState({sorted : sortObj}, () => {
      this.fetchData();
    });
  }
  sortByKey = (string) => {
    switch (string) {
    case "timestamp": return "Last Updated";
      break;
    case "name" : return "Name";
      break;
    default: return "Last Updated";
    }
  }
  handleOnEnter(s){
    s.renderCodemirror = true;
    this.forceUpdate();
  }
  handleOnExit(s){
    s.renderCodemirror = false;
    this.setState(this.state);
  }
  handleSelect(s, k, e){
    let {schemaData} = this.state;
    let schema = _.find(schemaData,{id: s.id});
    let obj = {};
    schema.collapsed = !s.collapsed;
    obj.schemaData = schemaData;
    this.setState(obj);
  }
  selectVersion(v) {
    let {schemaData} = this.state;
    let obj = _.find(schemaData, {schemaName: v.schemaName});
    obj.currentVersion = v.versionId;
    this.setState({schemaData: schemaData});
  }
  handleAddSchema() {
    this.setState({
      modalTitle: 'Add New Schema'
    }, () => {
      this.refs.schemaModal.show();
    });
  }
  handleAddVersion(schemaObj) {
    let obj = _.find(schemaObj.versionsArr, {versionId: schemaObj.currentVersion});
    this.schemaObj = {
      schemaName: schemaObj.schemaName,
      description: obj ? obj.description : '',
      schemaText: obj ? obj.schemaText : '',
      versionId: obj ? obj.versionId : ''
    };
    this.setState({
      modalTitle: 'Edit Version'
    }, () => {
      this.refs.versionModal.show();
    });
  }
  handleExpandView(schemaObj) {
    let obj = _.find(schemaObj.versionsArr, {versionId: schemaObj.currentVersion});
    this.schemaText = obj.schemaText;
    this.setState({
      modalTitle: obj.schemaName,
      expandSchema: true
    }, () => {
      this.setState({ expandSchema: true});
    });
  }
  handleSaveVersion() {
    if (this.refs.addVersion.validateData()) {
      this.refs.addVersion.handleSave().then((versions) => {
        if(versions && versions.compatible === false){
          FSReactToastr.error(<CommonNotification flag="error" content={versions.errorMessage}/>, '', toastOpt);
        } else {
          if (versions.responseMessage !== undefined) {
            FSReactToastr.error(
              <CommonNotification flag="error" content={versions.responseMessage}/>, '', toastOpt);
          } else {
            this.refs.versionModal.hide();
            this.fetchData();
            let msg = "Version added successfully";
            if (this.state.modalTitle === 'Edit Version') {
              msg = "Version updated successfully";
            }
            if(versions === this.schemaObj.versionId) {
              msg = "The schema version is already present";
              FSReactToastr.info(
                <strong>{msg}</strong>
              );
            } else {
              FSReactToastr.success(
                <strong>{msg}</strong>
              );
            }
          }
        }
      });
    }
  }
  handleSave() {
    if (this.refs.addSchema.validateData()) {
      this.refs.addSchema.handleSave().then((schemas) => {
        if (schemas.responseMessage !== undefined) {
          FSReactToastr.error(
            <CommonNotification flag="error" content={schemas.responseMessage}/>, '', toastOpt);
        } else {
          this.refs.schemaModal.hide();
          this.fetchData();
          let msg = "Schema added successfully";
          if (this.state.id) {
            msg = "Schema updated successfully";
          }
          FSReactToastr.success(
            <strong>{msg}</strong>
          );
        }
      });
    }
  }

  handlePagination = (eventKey) => {
    this.setState({
      activePage: eventKey
    });
  }

  getActivePageData = (allData, activePage, pageSize) => {
    activePage = activePage - 1;
    const startIndex = activePage*pageSize;
    return allData.slice(startIndex, startIndex+pageSize);
  }

  handleCompareVersions(schemaObj) {
    this.schemaObj = schemaObj;
    this.setState({
      modalTitle: 'Compare Schema Versions',
      showDiffModal: true
    });
  }

  render() {
    const jsonoptions = {
      lineNumbers: true,
      mode: "application/json",
      styleActiveLine: true,
      gutters: ["CodeMirror-lint-markers"],
      lint: false,
      readOnly: true,
      theme: 'default no-cursor schema-editor'
    };
    const schemaViewOptions = {
      lineNumbers: true,
      mode: "application/json",
      styleActiveLine: true,
      gutters: ["CodeMirror-lint-markers"],
      lint: false,
      readOnly: true,
      theme: 'default no-cursor schema-editor expand-schema'
    };
    const {slideInput, schemaData, activePage, pageSize} = this.state;
    const sortTitle = <span>Sort:<span className="font-blue-color">&nbsp;{this.state.sorted.text}</span></span>;
    var schemaEntities = schemaData;
    /*if(filterValue.trim() !== ''){
      schemaEntities = this.filterSchema(schemaData, filterValue);
    }*/
    const activePageData = this.getActivePageData(schemaEntities, activePage, pageSize);

    const panelContent = activePageData.map((s, i)=>{
      var btnClass = this.getBtnClass(s.compatibility);
      var iconClass = this.getIconClass(s.compatibility);
      var versionObj = _.find(s.versionsArr, {versionId: s.currentVersion});
      var totalVersions = s.versionsArr.length;
      var sortedVersions =  Utils.sortArray(s.versionsArr.slice(), 'versionId', false);
      var header = (
        <div>
        <span className={`hb ${btnClass} schema-status-icon`}><i className={iconClass}></i></span>
        <div className="panel-sections first fluid-width-15">
            <h4 ref="schemaName" className="schema-name" title={s.schemaName}>{Utils.ellipses(s.schemaName, this.schemaNameTagWidth)}</h4>
            <p className={`schema-status ${s.compatibility.toLowerCase()}`}>{s.compatibility}</p>
        </div>
        <div className="panel-sections">
            <h6 className="schema-th">Type</h6>
            <h4 className={`schema-td ${!s.collapsed ? "font-blue-color" : ''}`}>{s.type}</h4>
        </div>
        <div className="panel-sections">
            <h6 className="schema-th">Group</h6>
            <h4 ref="schemaGroup" className={`schema-td ${!s.collapsed ? "font-blue-color" : ''}`} title={s.schemaGroup}>{Utils.ellipses(s.schemaGroup, this.schemaGroupTagWidth)}</h4>
        </div>
        <div className="panel-sections">
            <h6 className="schema-th">Version</h6>
            <h4 className={`schema-td ${!s.collapsed ? "font-blue-color" : ''}`}>{s.versionsArr.length}</h4>
        </div>
        <div className="panel-sections">
            <h6 className="schema-th">Serializer & Deserializer</h6>
            <h4 className={`schema-td ${!s.collapsed ? "font-blue-color" : ''}`}><Link to={"/schemas/"+s.schemaName+'/serdes'} style={{display:'inline'}}>{s.serDesInfos.length}</Link></h4>
        </div>
        <div className="panel-sections" style={{'textAlign': 'right'}}>
            <a className="collapsed collapseBtn" role="button" aria-expanded="false">
              <i className={s.collapsed ? "collapseBtn fa fa-chevron-down" : "collapseBtn fa fa-chevron-up"}></i>
            </a>
        </div>
        </div>
      );
      const expandButton = ' ' || <button key="e.3" type="button" className="btn btn-link btn-expand-schema" onClick={this.handleExpandView.bind(this, s)}>
        <i className="fa fa-arrows-alt"></i>
      </button>;

      return (<Panel
            header={header}
            headerRole="tabpanel"
            key={i}
            collapsible
            expanded={s.collapsed ? false : true}
            onSelect={this.handleSelect.bind(this, s)}
            onEntered={this.handleOnEnter.bind(this, s)}
            onExited={this.handleOnExit.bind(this, s)}
        >
            {s.collapsed ?
            '': (versionObj ? (<div className="panel-registry-body">
                    <div className="row">
                        <div className="col-sm-3">
                            <h6 className="schema-th">Description</h6>
                            <p>{versionObj.description}</p>
                        </div>
                        <div className="col-sm-6">
                            {s.renderCodemirror ?
                            (s.evolve ? ([<h6 key="e.1" className="version-number-text">VERSION&nbsp;{versionObj.versionId}</h6>,
                              <button key="e.2" type="button" className="btn btn-link btn-edit-schema" onClick={this.handleAddVersion.bind(this, s)}>
                                <i className="fa fa-pencil"></i>
                              </button>,
                              expandButton]) : expandButton)
                            : ''
                            }
                            {s.renderCodemirror ?
                                (<ReactCodemirror
                                    ref="JSONCodemirror"
                                    value={JSON.stringify(JSON.parse(versionObj.schemaText), null, ' ')}
                                    options={jsonoptions}
                                />)
                            : (<div className="col-sm-12">
                                    <div className="loading-img text-center" style={{marginTop : "50px"}}>
                                        <img src="../ui/styles/img/start-loader.gif" alt="loading" />
                                    </div>
                              </div>)
                            }
                        </div>
                <div className="col-sm-3">
                    <h6 className="schema-th">Change Log</h6>
                    <ul className="version-tree">
                        {
                        sortedVersions.map((v, i)=>{
                          return (
                              <li onClick={this.selectVersion.bind(this, v)} className={s.currentVersion === v.versionId? "clearfix current" : "clearfix"} key={i}>
                                <a className={s.currentVersion === v.versionId? "hb version-number" : "hb default version-number"}>v{v.versionId}</a>
                                <p>
                                  <span className="log-time-text">{Utils.splitTimeStamp(new Date(v.timestamp))}</span>
                                  <br/>
                                </p>
                                <ChangeState
                                  version={v}
                                  StateMachine={this.StateMachine}
                                  showEditBtn={s.currentVersion === v.versionId}
                                />
                              </li>
                          );
                        })
                      }
                    </ul>
                    {sortedVersions.length > 1 ?
                      <a className="compare-version" onClick={this.handleCompareVersions.bind(this, s)}>COMPARE VERSIONS</a> : ''
                    }
                </div>
            </div>
    </div>) :
    (<div className="panel-registry-body">
      <div className="row">
        {s.evolve ?
        ([<div className="col-sm-3" key="v.k.1">
            <h6 className="schema-th">Description</h6>
            <p></p>
        </div>,
          <div className="col-sm-6" key="v.k.2">
              {s.renderCodemirror ?
                <button type="button" className="btn btn-link btn-add-schema" onClick={this.handleAddVersion.bind(this, s)}>
                <i className="fa fa-pencil"></i>
                </button>
                : ''
              }
              {s.renderCodemirror ?
                (<ReactCodemirror
                  ref="JSONCodemirror"
                  value=""
                  options={jsonoptions}
                />)
                : (<div className="col-sm-12">
                    <div className="loading-img text-center" style={{marginTop : "50px"}}>
                      <img src="../ui/styles/img/start-loader.gif" alt="loading" />
                    </div>
                </div>)
              }
          </div>,
          <div className="col-sm-3" key="v.k.3">
            <h6 className="schema-th">Change Log</h6>
          </div>])
          : <div style={{'textAlign': 'center'}}>NO DATA FOUND</div>
        }
      </div>
    </div>)
            )}
</Panel>
      );
    });


    return (
      <BaseContainer routes={this.props.routes} onLandingPage="false" breadcrumbData={this.breadcrumbData} headerContent={'All Schemas'}>
          <div id="add-schema">
              <button role="button" type="button" className="actionAddSchema hb lg success" onClick={this.handleAddSchema.bind(this)}>
                  <i className="fa fa-plus"></i>
              </button>
          </div>
          {!this.state.loading && this.state.dataFound ?
            <div className="wrapper animated fadeIn">
              <div className="page-title-box row no-margin">
                  <div className="col-md-3 col-md-offset-6 text-right">
                    <FormGroup>
                       <InputGroup>
                         <FormControl type="text" placeholder="Search by name" value={this.state.filterValue} onChange={this.onFilterChange} onKeyPress={this.onFilterKeyPress} className="" />
                           <InputGroup.Addon>
                             <i className="fa fa-search"></i>
                           </InputGroup.Addon>
                         </InputGroup>
                     </FormGroup>
                  </div>
                  <div className="col-md-2 text-center">
                    <DropdownButton title={sortTitle}
                      id="sortDropdown"
                      className="sortDropdown"
                    >
                        <MenuItem active={this.state.sorted.key == 'name' ? true : false} onClick={this.onSortByClicked.bind(this,"name")}>
                            &nbsp;Name
                        </MenuItem>
                        <MenuItem active={this.state.sorted.key == 'timestamp' ? true : false} onClick={this.onSortByClicked.bind(this,"timestamp")}>
                            &nbsp;Last Update
                        </MenuItem>
                    </DropdownButton>
                  </div>
              </div>

              {!this.state.loading && schemaEntities.length ?
                <div className="row">
                    <div className="col-md-12">
                      <PanelGroup
                          bsClass="panel-registry"
                          role="tablist"
                      >
                        {panelContent}
                      </PanelGroup>
                      {schemaEntities.length > pageSize
                        ?
                        <div className="text-center">
                          <Pagination
                            prev
                            next
                            first
                            last
                            ellipsis
                            boundaryLinks
                            items={Math.ceil(schemaEntities.length/pageSize)}
                            maxButtons={5}
                            activePage={this.state.activePage}
                            onSelect={this.handlePagination} />
                        </div>
                        : null
                       }
                    </div>
                </div>
                : !this.state.loading ? <NoData /> : null}
            </div>
            : !this.state.loading ? <NoData /> : null}

          {this.state.loading ?
            <div className="col-sm-12">
              <div className="loading-img text-center" style={{marginTop : "50px"}}>
                <img src="../ui/styles/img/start-loader.gif" alt="loading" />
              </div>
            </div>
            : null}

        <FSModal ref="schemaModal" bsSize="large" data-title={this.state.modalTitle} data-resolve={this.handleSave.bind(this)}>
          <SchemaInfoForm ref="addSchema"/>
        </FSModal>
        <FSModal ref="versionModal" data-title={this.state.modalTitle} data-resolve={this.handleSaveVersion.bind(this)}>
          <SchemaVersionForm ref="addVersion" schemaObj={this.schemaObj}/>
        </FSModal>
        <Modal dialogClassName="modal-xl" ref="expandSchemaModal" bsSize="large" show={this.state.expandSchema} onHide={()=>{this.setState({ expandSchema: false });}}>
          <Modal.Header closeButton>
            <Modal.Title>{this.state.modalTitle}</Modal.Title>
          </Modal.Header>
          <Modal.Body>
            {this.state.expandSchema ?
            <ReactCodemirror
              ref="JSONCodemirror"
              value={JSON.stringify(JSON.parse(this.schemaText), null, ' ')}
              options={schemaViewOptions}
            /> : ''}
          </Modal.Body>
          <Modal.Footer>
            <Button onClick={()=>{this.setState({ expandSchema: false });}}>Close</Button>
          </Modal.Footer>
        </Modal>
        <Modal ref="schemaDiffModal" bsSize="large" show={this.state.showDiffModal} onHide={()=>{this.setState({ showDiffModal: false });}}>
          <Modal.Header closeButton>
            <Modal.Title>{this.state.modalTitle}</Modal.Title>
          </Modal.Header>
          <Modal.Body>
            <SchemaVersionDiff ref="compareVersion" schemaObj={this.schemaObj}/>
          </Modal.Body>
          <Modal.Footer>
            <Button onClick={()=>{this.setState({ showDiffModal: false });}}>Close</Button>
          </Modal.Footer>
        </Modal>
      </BaseContainer>

    );
  }
}
