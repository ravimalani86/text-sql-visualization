import 'zone.js';
import { createCustomElement } from '@angular/elements';
import { createApplication } from '@angular/platform-browser';
import { PlotlyChartComponent } from './app/plotly-chart.component';

async function definePlotlyChartElement(): Promise<void> {
  const app = await createApplication({ providers: [] });
  const element = createCustomElement(PlotlyChartComponent, { injector: app.injector });

  if (!customElements.get('plotly-chart')) {
    customElements.define('plotly-chart', element);
  }
}

definePlotlyChartElement().catch((err) => console.error(err));
