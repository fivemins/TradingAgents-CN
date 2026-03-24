import type { ReactNode } from "react";
import { NavLink } from "react-router-dom";

interface AppLayoutProps {
  children: ReactNode;
}

const navItems = [
  { to: "/", label: "仪表盘" },
  { to: "/analyze", label: "单股分析" },
  { to: "/overnight", label: "隔夜推荐" },
  { to: "/tasks", label: "任务中心" }
];

export function AppLayout({ children }: AppLayoutProps) {
  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark">TA</div>
          <div>
            <div className="brand-title notranslate" translate="no" lang="en">
              TradingAgents
            </div>
            <div className="brand-subtitle">本地智能分析台</div>
          </div>
        </div>

        <nav className="nav-list">
          {navItems.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              className={({ isActive }) =>
                isActive ? "nav-item nav-item-active" : "nav-item"
              }
            >
              {item.label}
            </NavLink>
          ))}
        </nav>

        <div className="sidebar-footer">
          <p>本地单用户工作台</p>
          <span className="notranslate" translate="no" lang="en">
            FastAPI + React + SQLite
          </span>
        </div>
      </aside>

      <div className="main-shell">
        <header className="topbar">
          <div>
            <p className="eyebrow notranslate" translate="no" lang="en">
              TradingAgents CN
            </p>
            <h1>股票分析仪表盘</h1>
          </div>
          <div className="topbar-pill">后台任务 + 轮询同步</div>
        </header>

        <main className="content-shell">{children}</main>
      </div>
    </div>
  );
}
