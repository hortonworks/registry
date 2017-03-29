module.exports = {
  "parser": "babel-eslint",
  "rules": {
    "strict": 0
  },
  "env": {
    "browser": true,
    "es6": true,
    "jquery": true
  },
  "parserOptions": {
    "sourceType": "module"
  },
  "plugins": [
    "header",
    "react"
  ],
  "rules": {
    "header/header": [1, "config/header.js"],
    "comma-dangle": [
      "error",
      "never"
    ],
    "indent": [
      "error",
      2
    ],
    "linebreak-style": [
      "error",
      "windows"
    ],
    "semi": [
      "error",
      "always"
    ],
    /* Advanced Rules*/
    "no-unexpected-multiline": 2,
    "curly": [2,"all"]
  }
};
