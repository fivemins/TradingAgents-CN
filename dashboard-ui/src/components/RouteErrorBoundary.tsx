import { Component, type ErrorInfo, type ReactNode } from "react";

interface RouteErrorBoundaryProps {
  children: ReactNode;
  pageName: string;
  resetKey: string;
}

interface RouteErrorBoundaryState {
  error: Error | null;
}

export class RouteErrorBoundary extends Component<
  RouteErrorBoundaryProps,
  RouteErrorBoundaryState
> {
  state: RouteErrorBoundaryState = { error: null };

  static getDerivedStateFromError(error: Error): RouteErrorBoundaryState {
    return { error };
  }

  componentDidCatch(error: Error, errorInfo: ErrorInfo): void {
    console.error(`[RouteErrorBoundary] ${this.props.pageName}`, error, errorInfo);
  }

  componentDidUpdate(prevProps: RouteErrorBoundaryProps): void {
    if (prevProps.resetKey !== this.props.resetKey && this.state.error) {
      this.setState({ error: null });
    }
  }

  render(): ReactNode {
    if (this.state.error) {
      return (
        <div className="page-grid">
          <section className="panel panel-wide">
            <div className="panel-header">
              <div>
                <p className="eyebrow">页面保护</p>
                <h3>{this.props.pageName}加载失败</h3>
              </div>
            </div>
            <div className="error-banner">
              <strong>页面渲染时出现异常。</strong>
              <span>
                请刷新后重试；如果问题持续存在，系统会优先展示稳定空态，而不是整页空白。
              </span>
            </div>
          </section>
        </div>
      );
    }
    return this.props.children;
  }
}
