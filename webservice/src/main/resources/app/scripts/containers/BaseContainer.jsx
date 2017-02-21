import React, {Component} from 'react';
import Header from '../components/Header';
import Footer from '../components/Footer';
import {Link} from 'react-router';
import {Confirm} from '../components/FSModal'

export default class BaseContainer extends Component {

  constructor(props) {
      super(props);
      // Operations usually carried out in componentWillMount go here
      this.state = {}
  }

  render(){
    const routes = this.props.routes || [{path: "/", name: 'Home'}]
    return(
        <div>
            <Header
              onLandingPage={this.props.onLandingPage}
              headerContent={this.props.headerContent}
            />
            <section className={
                this.props.onLandingPage === "true" ?
                "landing-wrapper container" :
                "container-fluid wrapper animated fadeIn"
              }
            >
              {this.props.children}
            </section>
            <Footer
              routes={routes}
              breadcrumbData={this.props.breadcrumbData}
            />
            <Confirm ref="Confirm"/>
        </div>
    );
  }
}
