import { BrowserRouter, Route, Routes, useLocation } from "react-router-dom";

import { AppLayout } from "./components/Layout";
import { RouteErrorBoundary } from "./components/RouteErrorBoundary";
import { AnalyzePage } from "./pages/AnalyzePage";
import { DashboardPage } from "./pages/DashboardPage";
import { OvernightPage } from "./pages/OvernightPage";
import { TaskDetailPage } from "./pages/TaskDetailPage";
import { TasksPage } from "./pages/TasksPage";

function AppRoutes() {
  const location = useLocation();

  function withBoundary(pageName: string, element: JSX.Element) {
    return (
      <RouteErrorBoundary pageName={pageName} resetKey={location.pathname}>
        {element}
      </RouteErrorBoundary>
    );
  }

  return (
    <AppLayout>
      <Routes>
        <Route path="/" element={withBoundary("首页", <DashboardPage />)} />
        <Route path="/analyze" element={withBoundary("单股分析", <AnalyzePage />)} />
        <Route path="/overnight" element={withBoundary("隔夜推荐", <OvernightPage />)} />
        <Route path="/tasks" element={withBoundary("任务列表", <TasksPage />)} />
        <Route
          path="/tasks/:taskId"
          element={withBoundary("任务详情", <TaskDetailPage />)}
        />
      </Routes>
    </AppLayout>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <AppRoutes />
    </BrowserRouter>
  );
}
