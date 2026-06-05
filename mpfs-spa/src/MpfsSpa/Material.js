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
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import Dialog from "@mui/material/Dialog";
import DialogTitle from "@mui/material/DialogTitle";
import DialogContent from "@mui/material/DialogContent";
import DialogActions from "@mui/material/DialogActions";
import Badge from "@mui/material/Badge";
import { createTheme, ThemeProvider } from "@mui/material/styles";
import AddIcon from "@mui/icons-material/Add";
import AppRegistrationIcon from "@mui/icons-material/AppRegistration";
import AccountCircleIcon from "@mui/icons-material/AccountCircle";
import BlockIcon from "@mui/icons-material/Block";
import CheckCircleIcon from "@mui/icons-material/CheckCircle";
import CloseIcon from "@mui/icons-material/Close";
import DeleteIcon from "@mui/icons-material/Delete";
import EditIcon from "@mui/icons-material/Edit";
import ManageAccountsIcon from "@mui/icons-material/ManageAccounts";
import PlaylistAddCheckIcon from "@mui/icons-material/PlaylistAddCheck";
import RefreshIcon from "@mui/icons-material/Refresh";
import SaveIcon from "@mui/icons-material/Save";
import StopCircleIcon from "@mui/icons-material/StopCircle";
import SyncIcon from "@mui/icons-material/Sync";
import UndoIcon from "@mui/icons-material/Undo";
import WarningAmberIcon from "@mui/icons-material/WarningAmber";

// mk wraps a MUI component so PureScript can call it as
//   component (props :: {...}) (children :: Array JSX) :: JSX
// React.createElement accepts a React element array as variadic children;
// react-basic's JSX is a React node at runtime, so this is sound.
const mk = (Comp) => (props) => (children) =>
  React.createElement(Comp, props, ...children);

// Leaf component: props only, no children.
const mkLeaf = (Comp) => (props) => React.createElement(Comp, props);
const defaultComponent = (Comp) =>
  Comp && typeof Comp === "object" && Comp.default ? Comp.default : Comp;

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
export const tableContainer = mk(TableContainer);
export const table = mk(Table);
export const tableHead = mk(TableHead);
export const tableBody = mk(TableBody);
export const tableRow = mk(TableRow);
export const tableCell = mk(TableCell);
export const dialog = mk(Dialog);
export const dialogTitle = mk(DialogTitle);
export const dialogContent = mk(DialogContent);
export const dialogActions = mk(DialogActions);
export const badge = mk(Badge);

export const tab = mkLeaf(Tab);
export const textField = mkLeaf(TextField);
export const chip = mkLeaf(Chip);
export const divider = mkLeaf(Divider);
export const circularProgress = mkLeaf(CircularProgress);
export const cardHeader = mkLeaf(CardHeader);
export const listItemText = mkLeaf(ListItemText);
export const alertTitle = mkLeaf(AlertTitle);
export const addIcon = mkLeaf(defaultComponent(AddIcon));
export const appRegistrationIcon = mkLeaf(defaultComponent(AppRegistrationIcon));
export const accountCircleIcon = mkLeaf(defaultComponent(AccountCircleIcon));
export const blockIcon = mkLeaf(defaultComponent(BlockIcon));
export const checkCircleIcon = mkLeaf(defaultComponent(CheckCircleIcon));
export const closeIcon = mkLeaf(defaultComponent(CloseIcon));
export const deleteIcon = mkLeaf(defaultComponent(DeleteIcon));
export const editIcon = mkLeaf(defaultComponent(EditIcon));
export const manageAccountsIcon = mkLeaf(defaultComponent(ManageAccountsIcon));
export const playlistAddCheckIcon = mkLeaf(
  defaultComponent(PlaylistAddCheckIcon),
);
export const refreshIcon = mkLeaf(defaultComponent(RefreshIcon));
export const saveIcon = mkLeaf(defaultComponent(SaveIcon));
export const stopCircleIcon = mkLeaf(defaultComponent(StopCircleIcon));
export const syncIcon = mkLeaf(defaultComponent(SyncIcon));
export const undoIcon = mkLeaf(defaultComponent(UndoIcon));
export const warningAmberIcon = mkLeaf(defaultComponent(WarningAmberIcon));

// --- event-handler helpers --------------------------------------------------
// MUI Tabs.onChange is (event, value); we only want the new tab index.
export const _onTabChange = (handler) => (_event, value) => handler(value)();
// TextField/Input onChange; surface event.target.value to the handler.
export const _onValueChange = (handler) => (event) =>
  handler(event.target.value)();

export const defaultTheme = createTheme({
  palette: {
    mode: "light",
    primary: { main: "#255f85" },
    secondary: { main: "#7a4f27" },
    success: { main: "#2f6f50" },
    warning: { main: "#b46a1c" },
  },
  shape: { borderRadius: 6 },
});
