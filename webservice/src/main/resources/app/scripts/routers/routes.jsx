import React from 'react'
import { Router, Route, hashHistory, browserHistory, IndexRoute } from 'react-router'

import SchemaRegContainer from '../containers/Registry-Services/SchemaRegistry/SchemaRegistryContainer'

const onEnter = (nextState, replace, callback) => {
	callback();
}

export default (
  <Route path="/" component={null} name="Home" onEnter={onEnter}>
    <IndexRoute name="Schema Registry" component={SchemaRegContainer} onEnter={onEnter} />
    <Route path="schema-registry" name="Schema Registry" onEnter={onEnter}>
      <IndexRoute component={SchemaRegContainer} onEnter={onEnter} />
    </Route>
  </Route>
)