import {
  AfterViewInit,
  ChangeDetectionStrategy,
  Component,
  ElementRef,
  Input,
  OnChanges,
  OnDestroy,
  SimpleChanges,
  ViewChild,
} from '@angular/core';
import Plotly from 'plotly.js-dist-min';

export type PlotlyJsonConfig = {
  data: any[];
  layout?: any;
  config?: any;
};

@Component({
  selector: 'plotly-chart',
  standalone: true,
  template: `<div #host class="plotly-host"></div>`,
  styles: [
    `
      :host {
        display: block;
      }
      .plotly-host {
        width: 100%;
        height: 100%;
      }
    `,
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class PlotlyChartComponent implements AfterViewInit, OnChanges, OnDestroy {
  @Input() config: PlotlyJsonConfig | null = null;

  @ViewChild('host', { static: true })
  private host!: ElementRef<HTMLDivElement>;

  private viewReady = false;

  ngAfterViewInit(): void {
    this.viewReady = true;
    void this.render();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if ('config' in changes) {
      void this.render();
    }
  }

  ngOnDestroy(): void {
    try {
      Plotly.purge(this.host.nativeElement);
    } catch {
      // best-effort cleanup only
    }
  }

  private async render(): Promise<void> {
    if (!this.viewReady) return;

    const el = this.host.nativeElement;
    const cfg = this.config;

    if (!cfg || !Array.isArray(cfg.data)) {
      try {
        Plotly.purge(el);
      } catch {
        // ignore
      }
      return;
    }

    const data = cfg.data;
    const layout = cfg.layout ?? {};
    const plotConfig = {
      responsive: true,
      displayModeBar: false,
      displaylogo: false,
      ...(cfg.config ?? {}),
    };

    await Plotly.newPlot(el, data, layout, plotConfig);
  }
}

