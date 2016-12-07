<div class="row">
  <div class="col-md-2 pull-left ">
    <span class="label-medium-semibold">Total % Changes</span>
  </div>
  <div class="col-md-2">
    <input type="checkbox" id="show-details" {{#if this.showDetailsChecked}}checked{{/if}}>
    <label for="show-details" class="metric-label">See Contribution
      Details</label>
  </div>
  <div class="col-md-2">
    <input type="checkbox" id="show-cumulative" {{#if this.showCumulativeChecked}}checked{{/if}}>
    <label for="show-cumulative" class="metric-label">Show Cumulative</label>
  </div>
  <div class="col-md-6"></div>
</div>
<div class="row bottom-buffer">
  <div class="col-md-12">
    <span class="label-small-semibold">Click on a cell to drill down into its contribution breakdown.</span>
  </div>
</div>
<div class="row">
  <div id="wow-metric-table-placeholder"></div>
</div>
<div class="row">
  <div id="wow-metric-dimension-table-placeholder"></div>
</div>