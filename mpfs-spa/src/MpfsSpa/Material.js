import * as React from "react";

import CssBaseline from "@mui/material/CssBaseline";
import Container from "@mui/material/Container";
import Box from "@mui/material/Box";
import Stack from "@mui/material/Stack";
import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Button from "@mui/material/Button";
import IconButton from "@mui/material/IconButton";
import TextField from "@mui/material/TextField";
import AppBar from "@mui/material/AppBar";
import Toolbar from "@mui/material/Toolbar";
import Tabs from "@mui/material/Tabs";
import Tab from "@mui/material/Tab";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import CardActions from "@mui/material/CardActions";
import CardHeader from "@mui/material/CardHeader";
import List from "@mui/material/List";
import ListItem from "@mui/material/ListItem";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemText from "@mui/material/ListItemText";
import Chip from "@mui/material/Chip";
import Divider from "@mui/material/Divider";
import Alert from "@mui/material/Alert";
import AlertTitle from "@mui/material/AlertTitle";
import CircularProgress from "@mui/material/CircularProgress";
import Link from "@mui/material/Link";
import Tooltip from "@mui/material/Tooltip";
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
export const paper = mk(Paper);
export const typography = mk(Typography);
export const button = mk(Button);
export const iconButton = mk(IconButton);
export const appBar = mk(AppBar);
export const toolbar = mk(Toolbar);
export const tabs = mk(Tabs);
export const card = mk(Card);
export const cardContent = mk(CardContent);
export const cardActions = mk(CardActions);
export const list = mk(List);
export const listItem = mk(ListItem);
export const listItemButton = mk(ListItemButton);
export const alert = mk(Alert);
export const themeProvider = mk(ThemeProvider);
export const link = mk(Link);
export const tooltip = mk(Tooltip);

export const tab = mkLeaf(Tab);
export const textField = mkLeaf(TextField);
export const chip = mkLeaf(Chip);
export const divider = mkLeaf(Divider);
export const circularProgress = mkLeaf(CircularProgress);
export const cardHeader = mkLeaf(CardHeader);
export const listItemText = mkLeaf(ListItemText);
export const alertTitle = mkLeaf(AlertTitle);

// --- event-handler helpers --------------------------------------------------
// MUI Tabs.onChange is (event, value); we only want the new tab index.
export const _onTabChange = (handler) => (_event, value) => handler(value)();
// TextField/Input onChange; surface event.target.value to the handler.
export const _onValueChange = (handler) => (event) =>
  handler(event.target.value)();

export const defaultTheme = createTheme({
  palette: {
    mode: "light",
    primary: { main: "#1565c0" },
    secondary: { main: "#6a1b9a" },
  },
  shape: { borderRadius: 10 },
});
