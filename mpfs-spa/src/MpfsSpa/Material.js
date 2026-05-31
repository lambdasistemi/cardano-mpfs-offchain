import * as React from "react";

import CssBaseline from "@mui/material/CssBaseline";
import Container from "@mui/material/Container";
import Box from "@mui/material/Box";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";
import Button from "@mui/material/Button";
import AppBar from "@mui/material/AppBar";
import Toolbar from "@mui/material/Toolbar";
import { createTheme, ThemeProvider } from "@mui/material/styles";

// mk wraps a MUI component so PureScript can call it as
//   component (props :: {...}) (children :: Array JSX) :: JSX
// React.createElement accepts a React element array as variadic children;
// react-basic's JSX is a React node at runtime, so this is sound.
const mk = (Comp) => (props) => (children) =>
  React.createElement(Comp, props, ...children);

// Leaf component: props only, no children.
const mkLeaf = (Comp) => (props) => React.createElement(Comp, props);

export const cssBaseline = React.createElement(CssBaseline);
export const container = mk(Container);
export const box = mk(Box);
export const stack = mk(Stack);
export const typography = mk(Typography);
export const button = mk(Button);
export const appBar = mk(AppBar);
export const toolbar = mk(Toolbar);
export const themeProvider = mk(ThemeProvider);

export const defaultTheme = createTheme({
  palette: {
    mode: "light",
    primary: { main: "#1565c0" },
    secondary: { main: "#6a1b9a" },
  },
});
