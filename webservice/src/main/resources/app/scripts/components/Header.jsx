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
import {Link} from 'react-router';
import {Nav, Navbar, NavItem, NavDropdown, MenuItem} from 'react-bootstrap';
import {streamlinePort} from '../utils/Constants';

export default class Header extends Component {

  constructor(props) {
    super();
  }

  clickHandler = (eventKey) => {
    event.preventDefault();
    switch (eventKey) {
    case "3.2":
      this.context.router.push("schema-registry");
      break;
    }
  }

  render() {
    const userIcon = <i className="fa fa-user"></i>;
    const bigIcon = <i className="fa fa-chevron-down"></i>;
    const config = <i className="fa fa-cog"></i>;

    return (
      <Navbar inverse fluid={true}>
        <Navbar.Header>
          <Navbar.Brand>
            <a href="javascript:void(0);">
              <strong>SCHEMA REGISTRY</strong>
            </a>
          </Navbar.Brand>
          <Navbar.Toggle/>
        </Navbar.Header>
        <Navbar.Collapse>
          <Nav onSelect={this.clickHandler}>
            <NavDropdown id="dash_dropdown" eventKey="3" title={bigIcon} noCaret>
              <MenuItem eventKey="3.2">
                <i className="fa fa-file-code-o"></i>
                &nbsp;Schema Registry
              </MenuItem>
            </NavDropdown>
          </Nav>
          <Navbar.Text pullLeft>
            {this.props.headerContent}
          </Navbar.Text>
        </Navbar.Collapse>
      </Navbar>
    );
  }
}

Header.contextTypes = {
  router: React.PropTypes.object.isRequired
};
